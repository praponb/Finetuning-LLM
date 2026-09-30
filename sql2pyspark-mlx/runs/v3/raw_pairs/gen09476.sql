SELECT *
FROM (
    SELECT *, RANK() OVER (PARTITION BY order_id ORDER BY quantity) AS rnk
    FROM order_items
    WHERE order_id IN (7, 10, 38)
) AS t
WHERE rnk <= 2
