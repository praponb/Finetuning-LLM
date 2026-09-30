SELECT
    id,
    product_id,
    quantity,
    order_id,
    unit_price,
    FIRST_VALUE(order_id) OVER (PARTITION BY product_id ORDER BY quantity DESC, id) AS first_order_id,
    SUM(unit_price) OVER (PARTITION BY product_id ORDER BY unit_price, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_unit_price
FROM order_items;
