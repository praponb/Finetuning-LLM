SELECT id, order_id, cost
FROM (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY order_id ORDER BY cost DESC, id) AS pos
    FROM shipments
    WHERE order_id = 25
) AS t
WHERE pos = 1;
