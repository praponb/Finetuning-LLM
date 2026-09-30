SELECT
    order_id,
    product_id,
    SUM(unit_price * quantity) AS revenue,
    SUM(unit_price) AS total_unit_price
FROM order_items
GROUP BY order_id, product_id;
