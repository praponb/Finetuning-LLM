SELECT
    id,
    order_id,
    unit_price,
    quantity,
    LAG(unit_price, 2) OVER (PARTITION BY order_id ORDER BY unit_price, id) AS prev_unit_price,
    quantity - LAG(quantity, 1) OVER (ORDER BY unit_price, id) AS quantity_change
FROM order_items
WHERE product_id IS NOT NULL
ORDER BY id DESC;
