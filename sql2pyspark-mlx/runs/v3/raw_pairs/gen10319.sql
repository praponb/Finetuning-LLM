SELECT *
FROM (
    SELECT order_id, COUNT(DISTINCT cost) AS unique_statuss
    FROM shipments
    GROUP BY order_id
) AS s
WHERE unique_statuss < 8
ORDER BY unique_statuss DESC, order_id DESC
