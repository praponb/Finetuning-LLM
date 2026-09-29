WITH scoped AS (
    -- Scope the policies rows
    SELECT
        id,
        holder_id,
        product,
        premium,
        end_date,
        status,
        LENGTH(TRIM(product)) AS product_len,
        CASE WHEN status IN ('ACTIVE', 'LAPSED') THEN 'group_a' WHEN status = 'CANCELLED' THEN 'group_b' ELSE 'other' END AS status_group
    FROM policies
    WHERE premium >= 1541.8
),
with_parent AS (
    -- Join policyholders
    SELECT
        s.id,
        s.holder_id,
        s.product,
        s.premium,
        s.end_date,
        s.status,
        s.status_group,
        p.joined_on AS holder_joined_on,
        p.risk_score AS holder_risk_score
    FROM scoped s
    LEFT JOIN policyholders p ON p.id = s.holder_id
),
metrics AS (
    -- Running and moving aggregates
    SELECT
        *,
        RANK() OVER (PARTITION BY product ORDER BY holder_joined_on DESC, id) AS holder_joined_on_rank,
        MIN(premium) OVER w_end_date_month AS end_date_month_min_premium,
        ROUND(AVG(premium) OVER (PARTITION BY status_group ORDER BY holder_risk_score, id ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING), 2) AS premium_avg_3_prior,
        SIZE(COLLECT_SET(status) OVER w_end_date_month) AS distinct_status_in_end_date_month,
        ROUND(PERCENT_RANK() OVER w_product_by_holder_joined_on_2, 3) AS holder_joined_on_percent_rank,
        COUNT(*) OVER (ORDER BY premium ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_count,
        LEAD(holder_joined_on) OVER (ORDER BY premium ASC, id) AS next_holder_joined_on,
        LEAD(holder_joined_on, 2) OVER w_all_by_premium AS next2_holder_joined_on,
        SUM(premium) OVER (PARTITION BY MONTH(end_date)) AS end_date_month_sum_premium,
        COUNT(*) OVER w_status_product AS rows_in_status_product,
        SUM(premium) OVER (ORDER BY premium ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_premium
    FROM with_parent
    WINDOW w_end_date_month AS (PARTITION BY MONTH(end_date)), w_product_by_holder_joined_on_2 AS (PARTITION BY product ORDER BY holder_joined_on DESC), w_all_by_premium AS (ORDER BY premium ASC, id), w_status_product AS (PARTITION BY status, product)
),
first_rows AS (
    -- First row per group
    SELECT *
    FROM metrics
    WHERE holder_joined_on_rank = 1
)
-- Final projection
SELECT DISTINCT distinct_status_in_end_date_month, holder_joined_on
FROM first_rows
ORDER BY holder_joined_on DESC, distinct_status_in_end_date_month DESC
