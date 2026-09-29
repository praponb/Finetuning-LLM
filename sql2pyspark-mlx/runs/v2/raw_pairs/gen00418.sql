SELECT order_id, min_quantity
FROM (
    SELECT order_id, MIN(quantity) AS min_quantity
    FROM order_items
    WHERE quantity BETWEEN 6 AND 7
    GROUP BY order_id
) AS sub
WHERE min_quantity < 4.3
ORDER BY min_quantity DESC;
