SELECT status, id
FROM shipments
EXCEPT ALL
SELECT status, id
FROM shipments
WHERE order_id IN (1, 7, 8, 13);
