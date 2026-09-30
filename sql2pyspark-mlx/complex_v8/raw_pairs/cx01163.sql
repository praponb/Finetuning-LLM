WITH prepared AS (
    -- Scope the policies rows
    SELECT
        id,
        holder_id,
        product,
        premium,
        start_date,
        end_date
    FROM policies
    WHERE status NOT IN ('CANCELLED', 'EXPIRED')
      AND premium >= 1051.9
),
ranked AS (
    -- Running and moving aggregates
    SELECT
        *,
        NTILE(4) OVER (PARTITION BY product ORDER BY end_date DESC, id) AS end_date_quartile,
        LAG(end_date) OVER (PARTITION BY product ORDER BY premium, id) AS prev_end_date,
        ROUND(AVG(premium) OVER (PARTITION BY DATE_TRUNC('MONTH', start_date), holder_id), 2) AS start_date_month_holder_id_avg_premium,
        COUNT(*) OVER w_holder_id_start_date_month AS rows_in_holder_id_start_date_month,
        ROW_NUMBER() OVER w_product_by_premium AS rn,
        MAX(premium) OVER (PARTITION BY product) AS product_max_premium,
        MAX(premium) OVER (PARTITION BY product ORDER BY end_date DESC, id ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING) AS premium_max_3_prior,
        FIRST_VALUE(end_date) OVER (PARTITION BY product ORDER BY end_date DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_end_date
    FROM prepared
    WINDOW w_holder_id_start_date_month AS (PARTITION BY holder_id, MONTH(start_date)), w_product_by_premium AS (PARTITION BY product ORDER BY premium, id)
),
totals AS (
    -- Group-level totals
    SELECT
        rn,
        end_date_quartile,
        holder_id,
        SUM(premium) AS total_premium,
        MAX(end_date) AS last_end_date,
        COUNT(DISTINCT product) AS distinct_products,
        ROUND(SUM(start_date_month_holder_id_avg_premium) / NULLIF(SUM(rn), 0), 3) AS start_date_month_holder_id_avg_premium_to_rn_rat
    FROM ranked
    GROUP BY rn, end_date_quartile, holder_id
    HAVING AVG(rn) > 4
),
with_trends AS (
    -- Running and moving aggregates
    SELECT
        *,
        SIZE(COLLECT_SET(holder_id) OVER (PARTITION BY distinct_products)) AS distinct_holder_id_in_distinct_products,
        SUM(rn) OVER (PARTITION BY end_date_quartile, distinct_products ORDER BY last_end_date, rn, end_date_quartile, holder_id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS rn_sum_7
    FROM totals
)
SELECT
    rn,
    holder_id,
    total_premium,
    last_end_date,
    start_date_month_holder_id_avg_premium_to_rn_rat,
    distinct_holder_id_in_distinct_products,
    GREATEST(end_date_quartile, rn) AS greatest_end_date_quartile_rn,
    COALESCE(end_date_quartile, 0) AS end_date_quartile_filled
FROM with_trends
ORDER BY greatest_end_date_quartile_rn NULLS LAST
