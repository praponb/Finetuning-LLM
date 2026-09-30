WITH valid_products AS (
    SELECT *
    FROM products
    WHERE product_name LIKE '%Bot%'
),
products_stats AS (
    SELECT brand, ROUND(STDDEV(cost), 2) AS stddev_cost, COUNT(DISTINCT price) AS unique_suppliers
    FROM valid_products
    GROUP BY brand
)
SELECT brand, stddev_cost, unique_suppliers
FROM products_stats
ORDER BY brand;
