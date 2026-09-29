WITH base AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        *,
        CONCAT_WS('-', country, city) AS country_city_key,
        COALESCE(city, 'N/A') AS resolved_city
    FROM suppliers
    WHERE LOWER(supplier_name) LIKE '%ion%'
),
latest_match AS (
    -- Latest matching product per row (LATERAL ... LIMIT 1)
    SELECT
        s.id,
        s.country,
        s.city,
        s.rating,
        s.country_city_key,
        x.product_launch_date,
        x.product_price
    FROM base s
    LEFT JOIN LATERAL (
            SELECT c.launch_date AS product_launch_date, c.price AS product_price
            FROM products c
            WHERE c.supplier_id = s.id
            ORDER BY c.price DESC, c.id DESC
            LIMIT 1
        ) x ON TRUE
),
metrics AS (
    -- Ranking and gap features
    SELECT
        *,
        MIN(product_price) OVER (PARTITION BY city ORDER BY product_launch_date, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS last4_min_product_price,
        SUM(product_price) OVER (PARTITION BY city ORDER BY product_launch_date, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS last4_sum_product_price,
        AVG(product_price) OVER (PARTITION BY city ORDER BY product_launch_date, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS last4_avg_product_price,
        MAX(product_price) OVER (PARTITION BY city ORDER BY product_launch_date, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS last4_max_product_price,
        FIRST_VALUE(rating, TRUE) OVER (ORDER BY product_price, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_rating
    FROM latest_match
),
ranked AS (
    -- Keep one row per key (latest first)
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY country, city ORDER BY last4_sum_product_price DESC, id) AS row_num
        FROM metrics
    ) t
    WHERE row_num = 1
),
kept_keys AS (
    -- Set difference on keys
    SELECT country, id
    FROM ranked
    EXCEPT
    SELECT country, id
    FROM suppliers
    WHERE rating BETWEEN 2.8 AND 3.0
),
kept_rows AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM ranked s
    JOIN kept_keys k ON s.country = k.country AND s.id = k.id
),
enriched AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        SUM(rating) OVER (PARTITION BY city, country) AS city_sum_rating,
        RANK() OVER (ORDER BY rating NULLS LAST) AS rating_rank
    FROM kept_rows
)
-- Grouped metrics
SELECT
    CASE WHEN last4_sum_product_price >= 4600 THEN 'PREMIUM' ELSE 'STANDARD' END AS last4_sum_product_price_tier,
    DATE_TRUNC('WEEK', product_launch_date) AS week_start,
    DATE_TRUNC('YEAR', product_launch_date) AS year_start,
    COUNT(*) FILTER (WHERE city = 'Chiang Mai') AS chiang_mai_rows,
    ROUND(SUM(last4_min_product_price) / NULLIF(COUNT(CASE WHEN city = 'Singapore' THEN 1 END), 0), 2) AS last4_min_product_price_per_singapore,
    GROUPING(CASE WHEN last4_sum_product_price >= 4600 THEN 'PREMIUM' ELSE 'STANDARD' END) AS grouping_level
FROM enriched
GROUP BY GROUPING SETS ((CASE WHEN last4_sum_product_price >= 4600 THEN 'PREMIUM' ELSE 'STANDARD' END, DATE_TRUNC('WEEK', product_launch_date), DATE_TRUNC('YEAR', product_launch_date)), (CASE WHEN last4_sum_product_price >= 4600 THEN 'PREMIUM' ELSE 'STANDARD' END, DATE_TRUNC('WEEK', product_launch_date)), ())
ORDER BY last4_sum_product_price_tier NULLS LAST
