SELECT
    ss.id,
    ss.store_id,
    ss.units,
    ss.sale_date,
    p.category,
    p.stock_qty,
    p.product_name
FROM store_sales ss
LEFT OUTER JOIN products p ON p.id = ss.product_id
WHERE ss.units BETWEEN 12 AND 39 AND ss.revenue >= 1500
