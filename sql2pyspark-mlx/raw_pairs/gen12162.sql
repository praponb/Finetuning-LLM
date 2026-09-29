WITH cleaned AS (
    SELECT
        id,
        supplier_name,
        city,
        CONCAT(supplier_name, ' - ', country) AS supplier_name_country,
        COALESCE(city, 'UNKNOWN') AS city_clean
    FROM suppliers
    WHERE city IS NOT NULL
      AND LENGTH(supplier_name) > 7
),
with_latest AS (
    SELECT s.*, x.price
    FROM cleaned s
    LEFT JOIN LATERAL (
            SELECT c.price
            FROM products c
            WHERE c.supplier_id = s.id
            ORDER BY c.stock_qty DESC, c.id DESC
            LIMIT 1
        ) x ON TRUE
),
metrics AS (
    SELECT
        id,
        city,
        NTILE(3) OVER (ORDER BY price DESC, id) AS price_bucket,
        AVG(price) OVER (PARTITION BY city ORDER BY price, id ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING) AS price_avg_6_prior,
        LAG(price, 1) OVER (ORDER BY price, id) AS prev_price
    FROM with_latest
    WHERE supplier_name IS NOT NULL
),
eligible AS (
    SELECT s.*
    FROM metrics s
    WHERE NOT EXISTS (
            SELECT 1
            FROM products c
            WHERE c.supplier_id = s.id
              AND c.stock_qty > 307
        )
),
with_last AS (
    SELECT
        s.id,
        s.city,
        s.price_bucket,
        s.prev_price,
        top1.cost
    FROM eligible s
    JOIN LATERAL (
            SELECT c.cost
            FROM products c
            WHERE c.supplier_id = s.id
            ORDER BY c.price DESC, c.id DESC
            LIMIT 1
        ) top1 ON TRUE
)
SELECT
    city,
    CASE WHEN price_bucket >= 8 THEN 'LARGE' ELSE 'SMALL' END AS price_bucket_tier,
    PERCENTILE_APPROX(prev_price, 0.95) AS prev_price_p95,
    ROUND(SUM(cost) / NULLIF(COUNT(CASE WHEN city = 'London' THEN 1 END), 0), 2) AS cost_per_london,
    APPROX_PERCENTILE(prev_price, ARRAY(0.25, 0.5, 0.75)) AS prev_price_quartiles,
    AVG(prev_price) AS avg_prev_price,
    SORT_ARRAY(COLLECT_SET(city)) AS city_list
FROM with_last
GROUP BY city, price_bucket_tier;
