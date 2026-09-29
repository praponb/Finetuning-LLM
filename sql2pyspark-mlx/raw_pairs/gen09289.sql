SELECT
    id,
    unit_price,
    product_id,
    order_id
FROM order_items
WHERE quantity != 5 AND quantity BETWEEN 4 AND 6;
