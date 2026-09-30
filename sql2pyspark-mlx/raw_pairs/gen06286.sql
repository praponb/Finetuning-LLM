WITH base AS (
    -- Keep recent, valid rows
    SELECT *, LENGTH(supplier_name) AS supplier_name_length
    FROM suppliers
    WHERE city IS NOT NULL
),
metrics AS (
    -- Running and moving aggregates
    SELECT
        *,
        NTILE(3) OVER (PARTITION BY city, country ORDER BY rating, id) AS rating_bucket,
        MIN(supplier_name_length) OVER (PARTITION BY city) AS city_min_supplier_name_length,
        MIN(supplier_name_length) OVER (PARTITION BY country) AS country_min_supplier_name_length,
        FIRST_VALUE(supplier_name_length) IGNORE NULLS OVER (PARTITION BY city, country ORDER BY rating, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_supplier_name_length
    FROM base
),
cleaned AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        supplier_name,
        country,
        city,
        rating,
        supplier_name_length,
        rating_bucket,
        city_min_supplier_name_length,
        country_min_supplier_name_length,
        first_known_supplier_name_length,
        UPPER(country) AS country_upper,
        COALESCE(city, 'DIRECT') AS resolved_city
    FROM metrics
    WHERE supplier_name_length BETWEEN 55 AND 79
),
latest_match AS (
    -- Latest matching product per row (LATERAL ... LIMIT 1)
    SELECT
        s.id,
        s.city,
        s.supplier_name_length,
        s.rating_bucket,
        s.city_min_supplier_name_length,
        latest.product_launch_date,
        latest.stock_qty
    FROM cleaned s
    LEFT JOIN LATERAL (
            SELECT c.launch_date AS product_launch_date, c.stock_qty
            FROM products c
            WHERE c.supplier_id = s.id
            ORDER BY c.stock_qty DESC, c.id DESC
            LIMIT 1
        ) latest ON TRUE
),
scored AS (
    -- Ranking and gap features
    SELECT
        *,
        SUM(city_min_supplier_name_length) OVER (PARTITION BY city ORDER BY product_launch_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_city_min_supplier_name_length,
        SUM(supplier_name_length) OVER w_running AS running_supplier_name_length,
        DENSE_RANK() OVER (PARTITION BY city ORDER BY rating_bucket DESC NULLS FIRST) AS rating_bucket_dense_rank,
        LAST_VALUE(city_min_supplier_name_length) IGNORE NULLS OVER (PARTITION BY city ORDER BY city_min_supplier_name_length, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_city_min_supplier_name_length,
        NTILE(3) OVER (PARTITION BY city ORDER BY city_min_supplier_name_length DESC, id) AS city_min_supplier_name_length_bucket
    FROM latest_match
    WINDOW w_running AS (PARTITION BY MONTH(product_launch_date), city ORDER BY rating_bucket, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
)
-- Grouped metrics
SELECT
    DATE_TRUNC('MONTH', product_launch_date) AS activity_month,
    COUNT(DISTINCT city) AS distinct_cities,
    SIZE(COLLECT_SET(city)) AS num_cities
FROM scored
GROUP BY DATE_TRUNC('MONTH', product_launch_date)
ORDER BY activity_month
