SELECT
    ss.sale_date,
    ss.units,
    p.price,
    p.stock_qty,
    p.product_name
FROM store_sales ss
INNER JOIN products p ON ss.product_id = p.id
WHERE ss.revenue < 3200 AND ss.revenue BETWEEN 1400 AND 3500
