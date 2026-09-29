WITH dim_totals AS (
    SELECT
        s.country,
        src.brand,
        COUNT(*) AS cnt,
        ROUND(SUM(src.price), 2) AS total_price,
        GROUPING_ID() AS level_id
    FROM products src
    LEFT JOIN suppliers s ON src.supplier_id = s.id
    GROUP BY GROUPING SETS ((s.country, src.brand), (s.country), ())
)
SELECT
    country,
    brand,
    cnt,
    total_price,
    level_id
FROM dim_totals
ORDER BY cnt ASC
