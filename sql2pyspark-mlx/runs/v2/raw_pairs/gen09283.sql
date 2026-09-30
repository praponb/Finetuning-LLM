SELECT
    ss.id,
    ss.revenue,
    ss.store_id,
    p.category,
    p.product_name
FROM store_sales ss
JOIN products p ON p.id = ss.product_id
WHERE p.category IN ('Sports', 'Grocery', 'Books')
