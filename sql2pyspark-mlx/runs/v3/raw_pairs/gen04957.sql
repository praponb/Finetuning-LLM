SELECT
    id,
    quantity,
    product_id,
    order_id,
    CEIL(unit_price) AS unit_price_whole
FROM order_items
WHERE product_id IN (18, 36) OR quantity IS NULL;
