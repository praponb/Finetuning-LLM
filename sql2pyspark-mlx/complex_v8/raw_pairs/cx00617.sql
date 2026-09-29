WITH prepared AS (
    -- Scope the policies rows
    SELECT
        id,
        holder_id,
        product,
        premium,
        status,
        DATEDIFF(DATE '2025-12-31', start_date) AS days_since_start_date,
        CASE WHEN premium >= 2945.9 THEN 'HIGH' WHEN premium >= 1132.1 THEN 'MEDIUM' ELSE 'LOW' END AS premium_tier
    FROM policies
    WHERE premium <= 3521.8
      AND product IS NOT NULL
),
sequenced AS (
    -- Window metrics over several partitions
    SELECT
        *,
        LEAD(days_since_start_date) OVER (PARTITION BY status, premium_tier ORDER BY premium DESC, id) AS next_days_since_start_date,
        SUM(days_since_start_date) OVER w_holder_id_status AS holder_id_status_sum_days_since_start_date,
        ROUND(AVG(premium) OVER (PARTITION BY product ORDER BY days_since_start_date DESC, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW), 2) AS premium_avg_3,
        LAST_VALUE(premium) OVER (PARTITION BY holder_id ORDER BY premium ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_premium,
        MAX(days_since_start_date) OVER (PARTITION BY holder_id ORDER BY premium ASC, id ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING) AS days_since_start_date_max_6_prior,
        LAG(days_since_start_date, 2) OVER (PARTITION BY status, premium_tier ORDER BY premium DESC, id) AS prev2_days_since_start_date,
        ROUND(PERCENT_RANK() OVER (PARTITION BY holder_id ORDER BY premium ASC), 3) AS premium_percent_rank,
        DENSE_RANK() OVER w_status_premium_tier_by_premi_2 AS premium_dense_rank,
        LAST_VALUE(premium) IGNORE NULLS OVER (PARTITION BY holder_id ORDER BY premium ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_premium,
        SUM(premium) OVER (PARTITION BY status, premium_tier ORDER BY premium DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_premium,
        premium - LAG(premium) OVER (PARTITION BY status, premium_tier ORDER BY premium DESC, id) AS premium_change,
        LEAD(premium) OVER (PARTITION BY status, premium_tier ORDER BY premium DESC, id) AS next_premium,
        NTH_VALUE(premium, 2) OVER (PARTITION BY product ORDER BY days_since_start_date DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_premium,
        RANK() OVER (PARTITION BY product ORDER BY days_since_start_date DESC) AS days_since_start_date_rank,
        ROUND(AVG(premium) OVER (PARTITION BY status, premium_tier ORDER BY premium DESC, id ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING), 2) AS premium_avg_3_prior
    FROM prepared
    WINDOW w_holder_id_status AS (PARTITION BY holder_id, status), w_status_premium_tier_by_premi_2 AS (PARTITION BY status, premium_tier ORDER BY premium DESC)
),
top_rows AS (
    -- First row per group
    SELECT
        id,
        holder_id,
        product,
        premium,
        status,
        days_since_start_date,
        premium_tier,
        next_days_since_start_date,
        holder_id_status_sum_days_since_start_date,
        premium_avg_3,
        last_premium,
        days_since_start_date_max_6_prior,
        prev2_days_since_start_date,
        premium_percent_rank,
        last_known_premium,
        running_premium,
        premium_change,
        next_premium,
        second_premium,
        days_since_start_date_rank,
        premium_avg_3_prior
    FROM sequenced
    WHERE premium_dense_rank = 1
)
-- Final projection
SELECT
    id,
    holder_id,
    product,
    premium,
    status,
    days_since_start_date,
    premium_tier,
    next_days_since_start_date,
    last_premium,
    days_since_start_date_max_6_prior,
    prev2_days_since_start_date,
    premium_percent_rank,
    last_known_premium,
    premium_change,
    next_premium,
    days_since_start_date_rank,
    premium_avg_3_prior
FROM top_rows
ORDER BY next_premium DESC, prev2_days_since_start_date DESC
