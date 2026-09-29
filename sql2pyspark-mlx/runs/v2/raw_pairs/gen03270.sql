SELECT
    id,
    unit_price,
    order_id,
    product_id
FROM order_items
WHERE product_id IN (18, 30);
