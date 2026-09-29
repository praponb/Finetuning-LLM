SELECT
    ss.revenue,
    ss.sale_date,
    p.stock_qty,
    p.launch_date
FROM store_sales ss
INNER JOIN products p ON p.id = ss.product_id
WHERE p.price BETWEEN 500 AND 1190 AND ss.units <> 28
ORDER BY launch_date;
