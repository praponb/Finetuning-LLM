SELECT
    i.id,
    i.last_restock,
    ss.sale_date,
    ss.units
FROM inventory i
LEFT OUTER JOIN store_sales ss ON i.store_id = ss.store_id AND i.product_id = ss.product_id
WHERE ss.units < 34 OR i.last_restock >= '2024-03-01';
