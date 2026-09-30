SELECT
    id,
    quantity,
    unit_price,
    product_id,
    CAST(quantity AS STRING) quantity_num
FROM order_items
WHERE NOT (order_id IN (6, 12, 40) AND quantity BETWEEN 4 AND 6 AND quantity > 4);
