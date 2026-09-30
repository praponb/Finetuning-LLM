SELECT status
FROM payments
WHERE status IS NOT NULL
UNION
SELECT city
FROM suppliers
WHERE city IS NOT NULL
ORDER BY status;
