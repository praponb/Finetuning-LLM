SELECT
    order_id,
    SUM(unit_price) AS total_unit_price,
    COUNT(*) AS cnt,
    SUM(SUM(unit_price)) OVER (ORDER BY order_id) AS cumulative_unit_price
FROM order_items
GROUP BY order_id
