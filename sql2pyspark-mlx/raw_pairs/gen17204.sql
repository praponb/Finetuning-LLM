WITH not_flagged AS (
    SELECT s.*
    FROM suppliers s
    WHERE NOT EXISTS (
            SELECT 1
            FROM products ch
            WHERE ch.supplier_id = s.id
              AND ch.product_name IS NULL
        )
),
metrics AS (
    SELECT
        *,
        NTILE(5) OVER (PARTITION BY country ORDER BY rating DESC, id) AS rating_bucket,
        FIRST_VALUE(city, TRUE) OVER (PARTITION BY country, city ORDER BY rating, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_city,
        SUM(rating) OVER (PARTITION BY country ORDER BY rating, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_rating
    FROM not_flagged
),
kept_keys AS (
    SELECT id, city
    FROM metrics
    EXCEPT ALL
    SELECT id, city
    FROM suppliers
    WHERE LENGTH(supplier_name) > 6
),
kept_rows AS (
    SELECT s.*
    FROM metrics s
    INNER JOIN kept_keys k ON s.id = k.id AND s.city = k.city
),
kept AS (
    SELECT s.*
    FROM kept_rows s
    WHERE EXISTS (
            SELECT 1
            FROM products x
            WHERE x.supplier_id = s.id
              AND x.stock_qty > 345
        )
)
SELECT
    CASE WHEN rating >= 4.3 THEN 'LARGE' ELSE 'SMALL' END AS rating_tier,
    ROUND(SUM(rating_bucket) / NULLIF(COUNT(CASE WHEN country = 'DE' THEN 1 END), 0), 2) AS rating_bucket_per_de,
    SORT_ARRAY(COLLECT_SET(city)) AS city_list,
    COUNT(*) FILTER (WHERE city = 'Chiang Mai') AS chiang_mai_rows
FROM kept
GROUP BY CASE WHEN rating >= 4.3 THEN 'LARGE' ELSE 'SMALL' END
ORDER BY chiang_mai_rows DESC NULLS FIRST
