SELECT
    ss.revenue,
    ss.sale_date,
    ss.product_id,
    s.store_name
FROM store_sales ss
LEFT JOIN stores s ON s.id = ss.store_id
WHERE ss.revenue > 3900 AND ss.sale_date BETWEEN '2023-08-01' AND '2025-01-01'
ORDER BY revenue
