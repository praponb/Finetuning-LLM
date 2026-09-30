WITH recent_policys AS (
    -- Scope the policies rows
    SELECT
        id,
        holder_id,
        product,
        premium,
        start_date,
        end_date
    FROM policies
    WHERE premium > 2289.0
      AND holder_id IS NOT NULL
),
with_parent AS (
    -- Join policyholders
    SELECT
        s.id,
        s.holder_id,
        s.product,
        s.premium,
        s.start_date,
        s.end_date,
        p.joined_on AS holder_joined_on,
        p.region AS holder_region
    FROM recent_policys s
    JOIN policyholders p ON p.id = s.holder_id
    WHERE p.region = 'Central'
),
metrics AS (
    -- Rank and compare rows within groups
    SELECT
        id,
        holder_id,
        product,
        premium,
        start_date,
        end_date,
        holder_joined_on,
        holder_region,
        MAX(premium) OVER (PARTITION BY product, MONTH(end_date) ORDER BY holder_joined_on DESC, id ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING) AS premium_max_3_prior,
        ROUND(premium / NULLIF(SUM(premium) OVER w_holder_region, 0), 4) AS premium_share_of_holder_region,
        SUM(premium) OVER (PARTITION BY product ORDER BY premium, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_premium,
        SUM(premium) OVER (PARTITION BY holder_id) AS holder_id_sum_premium,
        ROUND(premium / NULLIF(SUM(premium) OVER w_holder_id, 0), 4) AS premium_share_of_holder_id,
        SUM(premium) OVER w_holder_region AS holder_region_sum_premium,
        MIN(premium) OVER w_holder_region AS holder_region_min_premium,
        COUNT(*) OVER (PARTITION BY holder_region) AS rows_in_holder_region,
        ROUND(CUME_DIST() OVER w_product_by_premium, 3) AS premium_cume_dist,
        LAG(end_date) OVER (PARTITION BY holder_region, DATE_TRUNC('MONTH', holder_joined_on) ORDER BY end_date ASC, id) AS prev_end_date,
        FIRST_VALUE(end_date) OVER (PARTITION BY holder_region, holder_id ORDER BY start_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_end_date,
        RANK() OVER (PARTITION BY product, MONTH(end_date) ORDER BY holder_joined_on DESC) AS holder_joined_on_rank
    FROM with_parent
    WINDOW w_holder_region AS (PARTITION BY holder_region), w_holder_id AS (PARTITION BY holder_id), w_product_by_premium AS (PARTITION BY product ORDER BY premium, id)
),
metrics_2 AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        SIZE(COLLECT_SET(product) OVER (PARTITION BY holder_region)) AS distinct_product_in_holder_region,
        NTILE(3) OVER (PARTITION BY product ORDER BY holder_joined_on_rank DESC, id) AS holder_joined_on_rank_tercile,
        ROUND(AVG(holder_id_sum_premium) OVER (PARTITION BY DATE_TRUNC('MONTH', start_date), DATE_TRUNC('MONTH', end_date) ORDER BY holder_joined_on, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW), 2) AS holder_id_sum_premium_avg_4,
        rows_in_holder_region - LAG(rows_in_holder_region) OVER (PARTITION BY DATE_TRUNC('MONTH', start_date), DATE_TRUNC('MONTH', end_date) ORDER BY holder_joined_on, id) AS rows_in_holder_region_change
    FROM metrics
)
SELECT
    id,
    holder_id,
    product,
    premium,
    start_date,
    holder_joined_on,
    premium_max_3_prior,
    premium_share_of_holder_region,
    running_premium,
    holder_id_sum_premium,
    premium_share_of_holder_id,
    holder_region_sum_premium,
    rows_in_holder_region,
    prev_end_date,
    first_end_date,
    holder_joined_on_rank,
    distinct_product_in_holder_region,
    holder_joined_on_rank_tercile,
    holder_id_sum_premium_avg_4
FROM metrics_2
ORDER BY running_premium DESC
