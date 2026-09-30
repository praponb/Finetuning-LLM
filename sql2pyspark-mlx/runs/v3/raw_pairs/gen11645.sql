SELECT
    i.id,
    i.quantity,
    i.product_id,
    ss.sale_date,
    ss.product_id AS store_sale_product_id
FROM inventory i
INNER JOIN store_sales ss ON i.store_id = ss.store_id AND i.product_id = ss.product_id
