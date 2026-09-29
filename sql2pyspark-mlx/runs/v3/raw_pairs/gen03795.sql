SELECT
    id,
    product_id,
    quantity,
    ROW_NUMBER() OVER (PARTITION BY product_id ORDER BY quantity DESC, id) AS row_num
FROM order_items
WHERE product_id = 22;
