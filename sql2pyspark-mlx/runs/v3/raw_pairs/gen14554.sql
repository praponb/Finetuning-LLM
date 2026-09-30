SELECT
    ss.units,
    ss.product_id,
    ss.sale_date,
    p.price,
    p.launch_date
FROM store_sales ss
RIGHT JOIN products p ON p.id = ss.product_id
WHERE YEAR(p.launch_date) = 2025 OR p.cost <= 470
ORDER BY price
