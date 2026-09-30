SELECT cost
FROM shipments
WHERE status IN ('DELIVERED', 'IN_TRANSIT', 'RETURNED')
UNION
SELECT amount
FROM payments
WHERE method IN ('card', 'cash', 'promptpay')
ORDER BY cost;
