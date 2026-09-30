SELECT
    i.store_id,
    i.quantity,
    ss.sale_date,
    ss.units,
    CEIL(i.quantity) AS quantity_whole
FROM inventory i
JOIN store_sales ss ON i.store_id = ss.store_id AND i.product_id = ss.product_id
ORDER BY ss.sale_date;
