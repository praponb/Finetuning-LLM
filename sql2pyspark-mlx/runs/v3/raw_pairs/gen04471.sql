SELECT
    i.id,
    i.store_id,
    ss.units,
    ss.sale_date,
    ss.store_id store_sale_store_id
FROM inventory i
INNER JOIN store_sales ss ON i.store_id = ss.store_id AND i.product_id = ss.product_id
WHERE i.product_id IN (4, 5, 6);
