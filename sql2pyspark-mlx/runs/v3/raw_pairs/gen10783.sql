SELECT
    CASE WHEN category IN ('Sports', 'Electronics') THEN 'core' ELSE 'other' END AS category_group,
    category,
    brand,
    SUM(CASE WHEN category = 'Electronics' THEN 1 ELSE 0 END) AS electronics_count,
    ROUND(AVG(cost), 2) AS avg_cost
FROM products
WHERE supplier_id IS NOT NULL AND category IN ('Grocery', 'Fashion', 'Books')
GROUP BY category_group, category, brand
ORDER BY category_group, category, brand
LIMIT 3;
