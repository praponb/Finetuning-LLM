SELECT
    id,
    order_id,
    product_id,
    unit_price,
    LAG(unit_price, 1) OVER (PARTITION BY order_id, product_id ORDER BY unit_price, id) AS prev_unit_price,
    unit_price - LAG(unit_price, 1, 0) OVER (PARTITION BY order_id ORDER BY unit_price, id) AS unit_price_change
FROM order_items
