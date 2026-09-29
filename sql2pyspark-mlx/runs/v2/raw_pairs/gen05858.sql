SELECT
    id,
    order_id,
    unit_price,
    RANK() OVER (PARTITION BY order_id ORDER BY unit_price DESC) AS rnk,
    LAG(unit_price, 1) OVER (PARTITION BY order_id ORDER BY unit_price, id) AS prev_unit_price
FROM order_items
WHERE order_id IN (9, 18);
