WITH with_last AS (
    -- Latest matching product per row (LATERAL ... LIMIT 1)
    SELECT s.*, x.product_price
    FROM suppliers s
    LEFT JOIN LATERAL (
            SELECT c.price AS product_price
            FROM products c
            WHERE c.supplier_id = s.id
            ORDER BY c.stock_qty DESC, c.id DESC
            LIMIT 1
        ) x ON TRUE
),
latest_match AS (
    -- Latest matching product per row (LATERAL ... LIMIT 1)
    SELECT
        s.*,
        top1.product_brand,
        top1.product_price_2
    FROM with_last s
    LEFT JOIN LATERAL (
            SELECT c.brand AS product_brand, c.price AS product_price_2
            FROM products c
            WHERE c.supplier_id = s.id
            ORDER BY c.launch_date DESC, c.id DESC
            LIMIT 1
        ) top1 ON TRUE
),
metrics AS (
    -- Ranking and gap features
    SELECT
        id,
        country,
        rating,
        product_brand,
        product_price_2,
        AVG(rating) OVER (PARTITION BY city ORDER BY product_price_2, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS last7_avg_rating,
        SUM(rating) OVER (PARTITION BY city ORDER BY product_price_2, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS last7_sum_rating,
        MAX(rating) OVER (PARTITION BY city ORDER BY product_price_2, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS last7_max_rating,
        MIN(rating) OVER (PARTITION BY city ORDER BY product_price_2, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS last7_min_rating
    FROM latest_match
),
kept_keys AS (
    -- Set difference on keys
    SELECT id, country
    FROM metrics
    EXCEPT
    SELECT id, country
    FROM suppliers
    WHERE country IN ('UK', 'SG', 'JP')
),
kept_rows AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM metrics s
    INNER JOIN kept_keys k ON s.id = k.id AND s.country = k.country
),
kept_keys_2 AS (
    -- Set difference on keys
    SELECT country, id
    FROM kept_rows
    EXCEPT ALL
    SELECT country, id
    FROM suppliers
    WHERE rating <= 2.8
),
verified AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM kept_rows s
    INNER JOIN kept_keys_2 k ON s.country = k.country AND s.id = k.id
)
-- Multi-level aggregation
SELECT
    country,
    product_brand,
    PERCENTILE_APPROX(last7_avg_rating, 0.9) AS last7_avg_rating_p90,
    COUNT_IF(country = 'UK') AS uk_events,
    APPROX_PERCENTILE(last7_avg_rating, ARRAY(0.25, 0.5, 0.75)) AS last7_avg_rating_quartiles,
    COUNT(DISTINCT country) AS distinct_countries,
    SIZE(COLLECT_SET(country)) AS num_countries,
    SUM(last7_max_rating) AS total_last7_max_rating
FROM verified
GROUP BY country, product_brand
HAVING COUNT_IF(country = 'UK') < 1000
ORDER BY uk_events DESC;
