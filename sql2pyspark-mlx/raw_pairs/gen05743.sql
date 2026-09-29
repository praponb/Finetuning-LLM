SELECT *
FROM (
    SELECT
        id,
        order_id,
        cost,
        ROW_NUMBER() OVER (PARTITION BY order_id ORDER BY cost DESC, id) AS rnk
    FROM shipments
    WHERE order_id IN (8, 15, 18, 35)
) AS t
WHERE rnk <= 5
ORDER BY 3 DESC;
