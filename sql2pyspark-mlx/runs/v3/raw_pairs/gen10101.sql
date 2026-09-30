WITH valid_products AS (
    SELECT *
    FROM products
    WHERE SIZE(tags) >= 1 OR price BETWEEN 370 AND 1020
),
brand_summary AS (
    SELECT brand, MIN(cost) AS min_cost, COUNT(*) AS cnt
    FROM valid_products
    GROUP BY brand
)
SELECT brand, min_cost, cnt
FROM brand_summary
ORDER BY min_cost DESC, brand ASC
LIMIT 10
