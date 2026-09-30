SELECT
    id,
    order_id,
    product_id,
    unit_price,
    ROW_NUMBER() OVER (PARTITION BY order_id, product_id ORDER BY unit_price DESC, id) AS row_num
FROM order_items
ORDER BY order_id ASC;
