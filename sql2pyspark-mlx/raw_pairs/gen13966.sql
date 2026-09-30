SELECT order_id, num_order_items, unit_price_range
FROM (
    SELECT order_id, COUNT(*) AS num_order_items, MAX(unit_price) - MIN(unit_price) AS unit_price_range
    FROM order_items
    GROUP BY order_id
) AS agg
WHERE unit_price_range < 55
ORDER BY unit_price_range DESC
