WITH recent_policys AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        holder_id,
        product,
        premium,
        start_date,
        end_date,
        status,
        CASE WHEN product IN ('HOME', 'LIFE') THEN 'group_a' WHEN product = 'HEALTH' THEN 'group_b' ELSE 'other' END AS product_group
    FROM policies
    WHERE premium > 1541.8
      AND status IN ('ACTIVE', 'CANCELLED')
),
with_activity AS (
    -- Compare against the overall avg
    SELECT *
    FROM recent_policys
    WHERE premium > (SELECT AVG(premium) FROM recent_policys)
),
metrics AS (
    -- Running and moving aggregates
    SELECT
        id,
        premium,
        start_date,
        status,
        product_group,
        DENSE_RANK() OVER w_all_by_end_date_ties AS end_date_dense_rank,
        ROUND(CUME_DIST() OVER (PARTITION BY product, status ORDER BY premium), 3) AS premium_cume_dist,
        SUM(premium) OVER (PARTITION BY holder_id) AS holder_id_sum_premium
    FROM with_activity
    WINDOW w_all_by_end_date_ties AS (ORDER BY end_date)
)
-- Native SQL PIVOT (long to wide)
SELECT *
FROM (
    SELECT end_date_dense_rank, product_group, status, premium
    FROM metrics
) src
PIVOT (
    MAX(premium)
    FOR status IN ('LAPSED', 'EXPIRED', 'ACTIVE', 'CANCELLED')
)
ORDER BY end_date_dense_rank, product_group
