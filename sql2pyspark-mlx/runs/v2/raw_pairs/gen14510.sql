SELECT
    i.product_id,
    i.store_id,
    ss.product_id AS store_sale_product_id,
    ss.units,
    ss.store_id AS store_sale_store_id,
    CAST(i.quantity AS STRING) AS quantity_str
FROM inventory i
LEFT OUTER JOIN store_sales ss ON i.store_id = ss.store_id AND i.product_id = ss.product_id
WHERE i.product_id = 5 AND ss.sale_date <= '2024-12-01';
