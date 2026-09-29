WITH prepared AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        holder_id,
        product,
        premium,
        start_date,
        end_date,
        status
    FROM policies
    WHERE start_date >= DATE '2024-06-19'
),
windowed AS (
    -- Previous / next values per key
    SELECT
        id,
        holder_id,
        product,
        premium,
        end_date,
        status,
        LAST_VALUE(start_date) OVER (PARTITION BY status, holder_id ORDER BY end_date NULLS LAST, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_start_date,
        MAX(premium) OVER (PARTITION BY DATE_TRUNC('MONTH', end_date), holder_id) AS end_date_month_holder_id_max_premium,
        SIZE(COLLECT_SET(product) OVER w_end_date_month_holder_id) AS distinct_product_in_end_date_month_holder_id,
        ROUND(premium - AVG(premium) OVER w_product, 2) AS premium_vs_product_avg,
        SIZE(COLLECT_SET(status) OVER (PARTITION BY DATE_TRUNC('MONTH', end_date), holder_id)) AS distinct_status_in_end_date_month_holder_id,
        FIRST_VALUE(start_date) IGNORE NULLS OVER (PARTITION BY status ORDER BY start_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_start_date,
        MAX(premium) OVER w_product AS product_max_premium,
        DENSE_RANK() OVER w_status_by_start_date_ties AS start_date_dense_rank,
        ROUND(AVG(premium) OVER (PARTITION BY status ORDER BY start_date, id ROWS BETWEEN 1 PRECEDING AND 1 PRECEDING), 2) AS premium_avg_1_prior,
        COUNT(*) OVER (PARTITION BY DATE_TRUNC('MONTH', end_date), holder_id) AS rows_in_end_date_month_holder_id,
        LEAD(start_date) OVER w_holder_id_by_end_date AS next_start_date,
        ROUND(premium - AVG(premium) OVER (PARTITION BY DATE_TRUNC('MONTH', end_date), holder_id), 2) AS premium_vs_end_date_month_holder_id_avg,
        SIZE(COLLECT_SET(status) OVER (PARTITION BY product)) AS distinct_status_in_product,
        LEAD(end_date, 3) OVER (PARTITION BY status, holder_id ORDER BY end_date NULLS LAST, id) AS next3_end_date
    FROM prepared
    WINDOW w_end_date_month_holder_id AS (PARTITION BY DATE_TRUNC('MONTH', end_date), holder_id), w_product AS (PARTITION BY product), w_status_by_start_date_ties AS (PARTITION BY status ORDER BY start_date), w_holder_id_by_end_date AS (PARTITION BY holder_id ORDER BY end_date ASC, id)
),
leaders AS (
    -- Top 2 per group
    SELECT
        id,
        holder_id,
        product,
        premium,
        end_date,
        status,
        last_start_date,
        end_date_month_holder_id_max_premium,
        distinct_product_in_end_date_month_holder_id,
        premium_vs_product_avg,
        distinct_status_in_end_date_month_holder_id,
        first_known_start_date,
        product_max_premium,
        premium_avg_1_prior,
        rows_in_end_date_month_holder_id,
        next_start_date,
        premium_vs_end_date_month_holder_id_avg,
        distinct_status_in_product,
        next3_end_date
    FROM windowed
    WHERE start_date_dense_rank <= 2
)
SELECT
    id,
    holder_id,
    product,
    premium,
    end_date,
    status,
    premium_vs_product_avg,
    distinct_status_in_end_date_month_holder_id,
    first_known_start_date,
    product_max_premium,
    premium_avg_1_prior,
    rows_in_end_date_month_holder_id,
    next_start_date,
    premium_vs_end_date_month_holder_id_avg,
    next3_end_date
FROM leaders
ORDER BY holder_id
