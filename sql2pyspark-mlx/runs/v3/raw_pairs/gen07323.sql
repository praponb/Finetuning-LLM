SELECT
    p.category,
    p.launch_date,
    oi.unit_price,
    SUBSTRING(p.brand, 1, 2) AS brand_prefix
FROM products p
JOIN order_items oi ON p.id = oi.product_id
WHERE p.price >= 830 AND p.category = 'Electronics'
