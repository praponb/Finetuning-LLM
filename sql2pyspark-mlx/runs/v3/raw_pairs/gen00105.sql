SELECT method
FROM payments
WHERE method IN ('bank_transfer', 'paypal')
UNION
SELECT method
FROM payments
WHERE order_id IS NOT NULL
ORDER BY method DESC
