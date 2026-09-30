SELECT
    ss.id,
    ss.store_id,
    ss.units,
    ss.sale_date,
    p.price
FROM store_sales ss
LEFT JOIN products p ON ss.product_id = p.id;
