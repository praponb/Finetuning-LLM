SELECT
    ss.id,
    ss.product_id,
    ss.revenue,
    p.price,
    s.rating,
    s.supplier_name
FROM store_sales ss
FULL OUTER JOIN products p ON ss.product_id = p.id
JOIN suppliers s ON s.id = p.supplier_id
WHERE p.category IN ('Sports', 'Books', 'Grocery') AND p.launch_date <= '2023-10-01'
