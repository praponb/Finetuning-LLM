WITH scoped AS (
    -- Scope the policies rows
    SELECT
        id,
        holder_id,
        product,
        premium,
        start_date,
        end_date,
        status,
        LOWER(REGEXP_REPLACE(TRIM(status), ' ', '_')) AS status_slug,
        DATE_FORMAT(end_date, 'MMM yyyy') AS end_date_label
    FROM policies
    WHERE (status NOT IN ('EXPIRED', 'CANCELLED') OR product = 'LIFE')
),
ranked AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        SIZE(COLLECT_SET(status) OVER w_end_date_month_product) AS distinct_status_in_end_date_month_product,
        MIN(premium) OVER (PARTITION BY holder_id ORDER BY premium, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS min_premium_so_far,
        FIRST_VALUE(start_date) OVER (PARTITION BY holder_id, product ORDER BY start_date DESC NULLS FIRST, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_start_date,
        ROUND(premium - AVG(premium) OVER (PARTITION BY DATE_TRUNC('MONTH', end_date), product), 2) AS premium_vs_end_date_month_product_avg,
        NTILE(5) OVER (PARTITION BY holder_id ORDER BY premium, id) AS premium_quintile,
        LAG(start_date) OVER w_holder_id_by_premium AS prev_start_date,
        LEAD(premium, 2, 0) OVER (PARTITION BY product ORDER BY end_date DESC, id) AS next2_premium,
        NTH_VALUE(premium, 2) OVER (PARTITION BY product ORDER BY end_date DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_premium,
        premium - LAG(premium) OVER (PARTITION BY holder_id ORDER BY premium, id) AS premium_change,
        ROUND(premium - AVG(premium) OVER (PARTITION BY product, status), 2) AS premium_vs_product_status_avg
    FROM scoped
    WINDOW w_end_date_month_product AS (PARTITION BY DATE_TRUNC('MONTH', end_date), product), w_holder_id_by_premium AS (PARTITION BY holder_id ORDER BY premium, id)
),
annotated AS (
    -- Label and score rows
    SELECT
        *,
        DATE_TRUNC('MONTH', first_start_date) AS first_start_date_month,
        FLOOR(premium_change / 5000) * 5000 AS premium_change_bucket
    FROM ranked
    WHERE end_date < DATE '2026-08-31'
),
windowed AS (
    -- Window metrics over several partitions
    SELECT
        *,
        MAX(premium_quintile) OVER (PARTITION BY premium_quintile, YEAR(first_start_date_month) ORDER BY premium_quintile, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS max_premium_quintile_so_far,
        SUM(min_premium_so_far) OVER (PARTITION BY premium_quintile, YEAR(first_start_date_month) ORDER BY premium_quintile, id ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING) AS min_premium_so_far_sum_3_prior,
        ROUND(premium_quintile - AVG(premium_quintile) OVER (PARTITION BY MONTH(prev_start_date), product), 2) AS premium_quintile_vs_prev_start_date_month_produc,
        FIRST_VALUE(next2_premium) OVER (PARTITION BY premium_quintile, YEAR(first_start_date_month) ORDER BY premium_quintile, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_next2_premium,
        ROUND(AVG(premium) OVER w_prev_start_date_month_produc, 2) AS prev_start_date_month_product_avg_premium
    FROM annotated
    WINDOW w_prev_start_date_month_produc AS (PARTITION BY MONTH(prev_start_date), product)
)
-- Final projection
SELECT
    id,
    holder_id,
    product,
    premium,
    start_date,
    end_date,
    status,
    end_date_label,
    distinct_status_in_end_date_month_product,
    first_start_date,
    premium_quintile,
    prev_start_date,
    next2_premium,
    second_premium,
    premium_change,
    premium_vs_product_status_avg,
    premium_change_bucket,
    max_premium_quintile_so_far,
    min_premium_so_far_sum_3_prior,
    premium_quintile_vs_prev_start_date_month_produc,
    first_next2_premium,
    prev_start_date_month_product_avg_premium,
    TRUNC(start_date, 'MM') AS start_date_month_start
FROM windowed
ORDER BY start_date_month_start
