WITH base AS (
    SELECT *
    FROM products
    WHERE price >= 1140
),
BrandSummary AS (
    SELECT brand, SUM(CASE WHEN category = 'Fashion' THEN 1 ELSE 0 END) AS fashion_count, ROUND(STDDEV(cost), 2) AS stddev_cost
    FROM base
    GROUP BY brand
)
SELECT *
FROM BrandSummary
WHERE fashion_count > 46
ORDER BY stddev_cost DESC;
