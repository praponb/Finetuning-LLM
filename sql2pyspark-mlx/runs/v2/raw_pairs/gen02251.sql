SELECT *
FROM (
    SELECT order_id, ROUND(AVG(unit_price), 2) avg_unit_price
    FROM order_items
    WHERE unit_price BETWEEN 120 AND 260
    GROUP BY order_id
) x
WHERE avg_unit_price > 49
ORDER BY 2 DESC;
