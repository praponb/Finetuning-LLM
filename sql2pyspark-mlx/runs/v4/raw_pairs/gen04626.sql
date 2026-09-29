SELECT id, order_id, cost
FROM (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY order_id ORDER BY cost, id) AS pos
    FROM shipments
) AS ranked
WHERE pos <= 2;
