SELECT status, SUM(CASE WHEN carrier = 'Flash' THEN cost ELSE 0 END) flash_cost
FROM shipments
WHERE order_id IS NOT NULL
GROUP BY status
HAVING SUM(CASE WHEN carrier = 'Flash' THEN cost ELSE 0 END) >= 58
ORDER BY status;
