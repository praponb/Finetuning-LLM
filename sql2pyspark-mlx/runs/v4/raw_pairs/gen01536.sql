SELECT *
FROM (
    SELECT order_id, SUM(unit_price * quantity) AS revenue, MAX(quantity) - MIN(quantity) AS quantity_range
    FROM order_items
    GROUP BY order_id
) AS agg
WHERE quantity_range > 77;
