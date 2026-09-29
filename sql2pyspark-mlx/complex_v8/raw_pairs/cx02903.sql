WITH filtered AS (
    -- Scope the policies rows
    SELECT
        id,
        holder_id,
        product,
        premium,
        end_date,
        CASE WHEN premium >= 2945.9 THEN 'large' WHEN premium >= 2319.8 THEN 'medium' ELSE 'small' END AS premium_tier
    FROM policies
    WHERE start_date >= DATE '2025-01-24'
),
labeled AS (
    -- Label and score rows
    SELECT
        id,
        product,
        premium,
        end_date,
        premium_tier,
        DATE_TRUNC('MONTH', end_date) AS end_date_month
    FROM filtered
),
metrics AS (
    -- Window metrics over several partitions
    SELECT
        id,
        product,
        end_date,
        end_date_month,
        ROUND(premium - AVG(premium) OVER w_end_date_month, 2) AS premium_vs_end_date_month_avg,
        SIZE(COLLECT_SET(premium_tier) OVER (PARTITION BY DATE_TRUNC('MONTH', end_date))) AS distinct_premium_tier_in_end_date_month,
        ROUND(PERCENT_RANK() OVER (ORDER BY premium), 3) AS premium_percent_rank,
        FIRST_VALUE(premium) OVER (ORDER BY premium, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_premium,
        LEAD(end_date_month, 2) OVER (ORDER BY premium, id) AS next2_end_date_month
    FROM labeled
    WINDOW w_end_date_month AS (PARTITION BY DATE_TRUNC('MONTH', end_date))
),
totals AS (
    -- Group-level totals
    SELECT
        DATE_TRUNC('MONTH', next2_end_date_month) AS next2_end_date_month_month,
        TO_DATE(end_date_month) AS end_date_month_day,
        COUNT(DISTINCT product) AS distinct_products,
        ROUND(AVG(distinct_premium_tier_in_end_date_month), 2) AS avg_distinct_premium_tier_in_end_date_month,
        MIN(end_date) AS first_end_date,
        ARRAY_SORT(COLLECT_SET(product)) AS product_list,
        COUNT(*) AS n_rows
    FROM metrics
    WHERE premium_vs_end_date_month_avg > -1000.0
    GROUP BY DATE_TRUNC('MONTH', next2_end_date_month), TO_DATE(end_date_month)
    HAVING AVG(distinct_premium_tier_in_end_date_month) > 4
)
-- Native SQL UNPIVOT (wide to long)
SELECT next2_end_date_month_month, end_date_month_day, field, reading
FROM (
    SELECT next2_end_date_month_month, end_date_month_day, n_rows, distinct_products
    FROM totals
) src
UNPIVOT INCLUDE NULLS (
    reading FOR field IN (n_rows, distinct_products)
) unp
ORDER BY next2_end_date_month_month, end_date_month_day, field
