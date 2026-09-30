WITH prepared_claims AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        policy_id,
        adjuster_id,
        claimed_amount,
        approved_amount,
        reported_at,
        LEAST(claimed_amount, approved_amount) AS least_claimed_amount_approved_amount,
        LEAST(approved_amount, claimed_amount) AS least_approved_amount_claimed_amount,
        ROUND(approved_amount * 0.9, 2) AS approved_amount_adj
    FROM claims
    WHERE status NOT IN ('REJECTED', 'IN_REVIEW')
),
without_matches AS (
    -- Compare against the overall max
    SELECT *
    FROM prepared_claims
    WHERE claimed_amount < (SELECT MAX(claimed_amount) FROM prepared_claims)
),
latest_per_key AS (
    -- Keep the latest row per adjuster_id
    SELECT
        id,
        policy_id,
        adjuster_id,
        claimed_amount,
        approved_amount,
        reported_at,
        least_claimed_amount_approved_amount,
        least_approved_amount_claimed_amount,
        approved_amount_adj
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY adjuster_id ORDER BY approved_amount_adj DESC, id DESC) AS row_num
        FROM without_matches
    ) ranked
    WHERE row_num = 1
),
derived AS (
    -- Label and score rows
    SELECT
        id,
        claimed_amount,
        approved_amount,
        reported_at,
        least_claimed_amount_approved_amount,
        least_approved_amount_claimed_amount,
        approved_amount_adj,
        COALESCE(claimed_amount, 0.0) AS claimed_amount_filled,
        DATEDIFF(DATE '2025-06-30', reported_at) AS days_since_reported_at
    FROM latest_per_key
),
sequenced AS (
    -- Rank and compare rows within groups
    SELECT
        id,
        claimed_amount,
        approved_amount,
        reported_at,
        least_claimed_amount_approved_amount,
        least_approved_amount_claimed_amount,
        days_since_reported_at,
        NTILE(4) OVER (ORDER BY days_since_reported_at DESC, id) AS days_since_reported_at_quartile,
        COUNT(*) OVER (PARTITION BY MONTH(reported_at)) AS rows_in_reported_at_month
    FROM derived
),
screened AS (
    -- Compare against the overall avg
    SELECT *
    FROM sequenced
    WHERE approved_amount > (SELECT AVG(approved_amount) FROM sequenced)
),
sequenced_2 AS (
    -- Previous / next values per key
    SELECT
        id,
        claimed_amount,
        approved_amount,
        reported_at,
        least_claimed_amount_approved_amount,
        days_since_reported_at,
        days_since_reported_at_quartile,
        rows_in_reported_at_month,
        FIRST_VALUE(rows_in_reported_at_month) IGNORE NULLS OVER (PARTITION BY days_since_reported_at_quartile ORDER BY rows_in_reported_at_month DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_rows_in_reported_at_month,
        MIN(rows_in_reported_at_month) OVER (PARTITION BY days_since_reported_at_quartile) AS days_since_reported_at_quartile_min_rows_in_repo,
        SUM(claimed_amount) OVER (PARTITION BY days_since_reported_at_quartile) AS days_since_reported_at_quartile_sum_claimed_amou,
        LEAD(approved_amount, 1, 0) OVER w_days_since_reported_at_quart_3 AS next_approved_amount,
        ROUND(claimed_amount - AVG(claimed_amount) OVER (PARTITION BY days_since_reported_at_quartile), 2) AS claimed_amount_vs_days_since_reported_at_quartil
    FROM screened
    WINDOW w_days_since_reported_at_quart_3 AS (PARTITION BY days_since_reported_at_quartile ORDER BY days_since_reported_at NULLS LAST, id)
)
SELECT
    id,
    claimed_amount,
    approved_amount,
    reported_at,
    days_since_reported_at,
    days_since_reported_at_quartile,
    rows_in_reported_at_month,
    first_known_rows_in_reported_at_month,
    days_since_reported_at_quartile_min_rows_in_repo,
    next_approved_amount
FROM sequenced_2
ORDER BY reported_at DESC
