WITH cleaned AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        policy_id,
        adjuster_id,
        claim_date,
        claim_type,
        claimed_amount,
        approved_amount,
        status,
        reported_at,
        DATE_TRUNC('WEEK', claim_date) AS claim_date_week,
        DATEDIFF(DATE '2025-12-31', reported_at) AS days_since_reported_at
    FROM claims
),
latest_per_key AS (
    -- Keep the latest row per adjuster_id, policy_id
    SELECT
        id,
        policy_id,
        adjuster_id,
        claim_date,
        claim_type,
        claimed_amount,
        approved_amount,
        status,
        reported_at,
        claim_date_week,
        days_since_reported_at
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY adjuster_id, policy_id ORDER BY approved_amount DESC, id DESC) AS recency_rank
        FROM cleaned
    ) ranked
    WHERE recency_rank = 1
),
windowed AS (
    -- Previous / next values per key
    SELECT
        *,
        COUNT(*) OVER w_reported_at_month AS rows_in_reported_at_month,
        MAX(approved_amount) OVER (PARTITION BY status ORDER BY approved_amount, id ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING) AS approved_amount_max_2_prior
    FROM latest_per_key
    WINDOW w_reported_at_month AS (PARTITION BY MONTH(reported_at))
)
-- Native SQL PIVOT (long to wide)
SELECT *
FROM (
    SELECT adjuster_id, reported_at, claim_type, rows_in_reported_at_month
    FROM windowed
) src
PIVOT (
    AVG(rows_in_reported_at_month)
    FOR claim_type IN ('flood', 'collision', 'medical')
)
ORDER BY adjuster_id, reported_at
