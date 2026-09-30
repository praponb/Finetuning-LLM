SELECT id, shipped_date
FROM shipments
WHERE order_id = 14
EXCEPT
SELECT id, shipped_date
FROM shipments
WHERE MONTH(shipped_date) IN (1, 3, 11);
