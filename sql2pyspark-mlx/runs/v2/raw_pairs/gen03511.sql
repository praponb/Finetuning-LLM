SELECT
    category,
    brand,
    unique_suppliers,
    grocery_count
FROM (
    SELECT
        category,
        brand,
        COUNT(DISTINCT price) unique_suppliers,
        SUM(CASE WHEN category = 'Grocery' THEN 1 ELSE 0 END) grocery_count
    FROM products
    GROUP BY category, brand
) x
WHERE unique_suppliers < 9
ORDER BY grocery_count, category, brand
LIMIT 3;
