SELECT
    i.store_id,
    i.product_id,
    ss.revenue,
    ss.store_id store_sale_store_id,
    ss.sale_date
FROM inventory i
LEFT OUTER JOIN store_sales ss ON i.store_id = ss.store_id AND i.product_id = ss.product_id
WHERE i.quantity >= 63 OR ss.sale_date >= '2024-07-01';
