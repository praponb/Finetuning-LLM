SELECT
    ss.store_id,
    ss.units,
    p.cost,
    p.stock_qty,
    p.brand
FROM store_sales ss
LEFT JOIN products p ON ss.product_id = p.id
ORDER BY brand, ss.units DESC;
