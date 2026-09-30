WITH prepared AS (
    -- Scope the policies rows
    SELECT
        id,
        holder_id,
        product,
        premium,
        start_date,
        end_date,
        status,
        CASE WHEN premium >= 3817.5 THEN 'large' WHEN premium >= 2111.3 THEN 'medium' ELSE 'small' END AS premium_tier
    FROM policies
),
with_parent AS (
    -- Enrich with dimension data
    SELECT s.*, p.joined_on AS holder_joined_on, p.holder_name
    FROM prepared s
    JOIN policyholders p ON p.id = s.holder_id
),
metrics AS (
    -- Previous / next values per key
    SELECT
        *,
        LEAD(end_date) OVER (ORDER BY holder_joined_on ASC, id) AS next_end_date,
        ROW_NUMBER() OVER (ORDER BY end_date ASC, id) AS rn,
        COUNT(*) OVER (PARTITION BY premium_tier, status) AS rows_in_premium_tier_status,
        NTH_VALUE(start_date, 2) OVER (ORDER BY holder_joined_on ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_start_date,
        DENSE_RANK() OVER w_all_by_holder_joined_on AS holder_joined_on_dense_rank,
        ROUND(premium / NULLIF(SUM(premium) OVER (PARTITION BY premium_tier, status), 0), 4) AS premium_share_of_premium_tier_status,
        MIN(premium) OVER (PARTITION BY status) AS status_min_premium,
        ROUND(AVG(premium) OVER (PARTITION BY holder_id, premium_tier ORDER BY holder_joined_on, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW), 2) AS premium_avg_4,
        ROUND(premium - AVG(premium) OVER w_premium_tier_status, 2) AS premium_vs_premium_tier_status_avg
    FROM with_parent
    WINDOW w_all_by_holder_joined_on AS (ORDER BY holder_joined_on ASC, id), w_premium_tier_status AS (PARTITION BY premium_tier, status)
)
-- Final projection
SELECT
    id,
    holder_id,
    product,
    status,
    premium_tier,
    holder_name,
    next_end_date,
    rows_in_premium_tier_status,
    premium_avg_4
FROM metrics
ORDER BY holder_name, premium_tier DESC NULLS FIRST
