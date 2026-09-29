WITH base AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        supplier_name,
        country,
        city,
        CONCAT(city, '/', country) AS city_country
    FROM suppliers
    WHERE city IS NOT DISTINCT FROM 'London'
),
not_flagged AS (
    -- Semi-join via a correlated subquery
    SELECT s.*
    FROM base s
    WHERE EXISTS (
            SELECT 1
            FROM products ch
            WHERE ch.supplier_id = s.id
              AND ARRAY_CONTAINS(ch.tags, 'premium')
        )
),
eligible AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM not_flagged s
    WHERE NOT EXISTS (
            SELECT 1
            FROM products ch
            WHERE ch.supplier_id = s.id
              AND ch.category = 'Books'
        )
),
latest_match AS (
    -- Latest matching product per row (LATERAL ... LIMIT 1)
    SELECT
        s.id,
        s.country,
        x.product_category,
        x.price
    FROM eligible s
    LEFT JOIN LATERAL (
            SELECT c.category AS product_category, c.price
            FROM products c
            WHERE c.supplier_id = s.id
            ORDER BY c.cost DESC, c.id DESC
            LIMIT 1
        ) x ON TRUE
),
valid AS (
    -- Keep recent, valid rows
    SELECT
        *,
        CASE WHEN country = 'TH' THEN 1 ELSE 0 END AS is_th,
        COALESCE(product_category, 'UNKNOWN') AS product_category_filled
    FROM latest_match
    WHERE product_category IS NOT NULL
),
metrics AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        COUNT(price) OVER w_hist AS last3_count_price,
        SUM(price) OVER w_hist AS last3_sum_price,
        AVG(price) OVER w_hist AS last3_avg_price
    FROM valid
    WINDOW w_hist AS (PARTITION BY country ORDER BY price, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW)
)
-- Multi-level aggregation
SELECT
    CASE WHEN last3_sum_price >= 4800 THEN 'LARGE' ELSE 'SMALL' END AS last3_sum_price_tier,
    CASE WHEN price >= 1100 THEN 'HIGH' ELSE 'LOW' END AS price_tier,
    PERCENTILE_APPROX(last3_sum_price, ARRAY(0.25, 0.5, 0.75)) AS last3_sum_price_quartiles,
    MAX(is_th) AS max_is_th,
    COUNT_IF(product_category = 'Books') AS books_events
FROM metrics
GROUP BY 1, 2
ORDER BY price_tier;
