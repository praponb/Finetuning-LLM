SELECT
    category,
    supplier_id,
    SUM(CASE WHEN category = 'Sports' THEN cost ELSE 0 END) sports_cost,
    MAX(price) - MIN(price) price_range
FROM products
WHERE brand = 'Umbrella'
GROUP BY category, supplier_id
HAVING price_range < 27
