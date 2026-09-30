SELECT *
FROM (
    SELECT order_id, product_id, SUM(quantity * unit_price) AS revenue
    FROM order_items
    WHERE quantity > 7
    GROUP BY order_id, product_id
) AS sub
WHERE revenue >= 45
ORDER BY revenue DESC;
