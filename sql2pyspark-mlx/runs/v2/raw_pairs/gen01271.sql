SELECT
    id,
    order_id,
    unit_price,
    AVG(unit_price) OVER (PARTITION BY order_id) AS order_id_avg_unit_price
FROM order_items
ORDER BY order_id DESC, id DESC
