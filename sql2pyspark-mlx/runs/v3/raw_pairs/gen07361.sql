SELECT status, 'account' AS source
FROM accounts
WHERE YEAR(opened_date) >= 2025
UNION ALL
SELECT status, 'payment' AS source
FROM payments
WHERE order_id = 19
