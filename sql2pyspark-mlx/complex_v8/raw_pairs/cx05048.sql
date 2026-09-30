WITH valid AS (
    -- Filter claims
    SELECT
        id,
        policy_id,
        adjuster_id,
        claim_date,
        claim_type,
        claimed_amount,
        approved_amount,
        reported_at
    FROM claims
    WHERE claim_type <> 'flood'
      AND policy_id >= 19
),
metrics AS (
    -- Window metrics over several partitions
    SELECT
        *,
        claimed_amount - LAG(claimed_amount) OVER (PARTITION BY policy_id ORDER BY claimed_amount DESC NULLS FIRST, id) AS claimed_amount_change,
        SUM(claimed_amount) OVER w_claim_type_reported_at_month AS claim_type_reported_at_month_sum_claimed_amount,
        ROUND(claimed_amount - AVG(claimed_amount) OVER w_claim_type_reported_at_month, 2) AS claimed_amount_vs_claim_type_reported_at_month_a,
        LAST_VALUE(approved_amount) OVER (PARTITION BY DATE_TRUNC('MONTH', reported_at), claim_type ORDER BY approved_amount, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_approved_amount,
        LEAD(approved_amount, 1, 0) OVER (PARTITION BY DATE_TRUNC('MONTH', reported_at), claim_type ORDER BY approved_amount, id) AS next_approved_amount,
        FIRST_VALUE(claim_date) IGNORE NULLS OVER (PARTITION BY adjuster_id ORDER BY reported_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_claim_date,
        RANK() OVER (PARTITION BY adjuster_id ORDER BY reported_at, id) AS reported_at_rank,
        COUNT(*) OVER (PARTITION BY claim_type, MONTH(reported_at)) AS rows_in_claim_type_reported_at_month,
        DENSE_RANK() OVER w_reported_at_month_claim_type_3 AS approved_amount_dense_rank
    FROM valid
    WINDOW w_claim_type_reported_at_month AS (PARTITION BY claim_type, MONTH(reported_at)), w_reported_at_month_claim_type_3 AS (PARTITION BY DATE_TRUNC('MONTH', reported_at), claim_type ORDER BY approved_amount)
),
top_per_group AS (
    -- First row per group
    SELECT
        id,
        policy_id,
        adjuster_id,
        claim_date,
        claim_type,
        claimed_amount,
        approved_amount,
        reported_at,
        claimed_amount_change,
        claim_type_reported_at_month_sum_claimed_amount,
        claimed_amount_vs_claim_type_reported_at_month_a,
        last_approved_amount,
        next_approved_amount,
        first_known_claim_date,
        reported_at_rank,
        rows_in_claim_type_reported_at_month
    FROM metrics
    WHERE approved_amount_dense_rank = 1
),
windowed AS (
    -- Previous / next values per key
    SELECT
        *,
        MAX(reported_at_rank) OVER (PARTITION BY YEAR(claim_date) ORDER BY reported_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS max_reported_at_rank_so_far,
        ROUND(PERCENT_RANK() OVER w_claim_date_year_by_reported_2, 3) AS reported_at_percent_rank,
        claimed_amount_change - LAG(claimed_amount_change) OVER (PARTITION BY YEAR(claim_date) ORDER BY reported_at, id) AS claimed_amount_change_change,
        FIRST_VALUE(last_approved_amount) IGNORE NULLS OVER (PARTITION BY YEAR(claim_date) ORDER BY reported_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_last_approved_amount,
        SUM(approved_amount) OVER (ORDER BY claim_type_reported_at_month_sum_claimed_amount DESC, id ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING) AS approved_amount_sum_6_prior
    FROM top_per_group
    WINDOW w_claim_date_year_by_reported_2 AS (PARTITION BY YEAR(claim_date) ORDER BY reported_at)
),
stats AS (
    -- Aggregate with conditional metrics
    SELECT
        reported_at_rank,
        TO_DATE(reported_at) AS reported_at_day,
        DATE_TRUNC('MONTH', first_known_claim_date) AS first_known_claim_date_month,
        COUNT(DISTINCT adjuster_id) AS distinct_adjuster_ids,
        MIN(reported_at_rank) AS min_reported_at_rank,
        MIN(claim_date) AS first_claim_date
    FROM windowed
    WHERE policy_id >= 24
    GROUP BY reported_at_rank, TO_DATE(reported_at), DATE_TRUNC('MONTH', first_known_claim_date)
)
-- Final projection
SELECT
    reported_at_rank,
    reported_at_day,
    first_known_claim_date_month,
    distinct_adjuster_ids,
    COALESCE(reported_at_rank, 0) AS reported_at_rank_filled,
    DATEDIFF(DATE '2025-06-30', first_claim_date) AS days_since_first_claim_date
FROM stats
ORDER BY first_known_claim_date_month NULLS LAST
