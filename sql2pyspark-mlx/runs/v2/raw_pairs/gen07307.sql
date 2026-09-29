SELECT
    brand,
    product_name,
    cost,
    ROUND(price * 0.9, 2) AS adjusted_price,
    SIZE(tags) AS tags_count
FROM products
WHERE supplier_id IN (13, 21, 31, 38) AND category IN ('Books', 'Grocery', 'Electronics')
ORDER BY product_name, cost DESC;
