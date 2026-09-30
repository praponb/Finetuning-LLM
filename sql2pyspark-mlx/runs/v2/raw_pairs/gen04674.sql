SELECT DISTINCT method, status
FROM payments
WHERE method IN ('promptpay', 'bank_transfer', 'card')
ORDER BY method, status
LIMIT 20;
