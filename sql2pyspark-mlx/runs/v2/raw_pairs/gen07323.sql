SELECT *, COALESCE(amount, 0) AS amount_filled, DATEDIFF(CURRENT_DATE(), paid_at) AS days_since_paid_at
FROM payments
WHERE status != 'FAILED'
ORDER BY 8 DESC, id
LIMIT 50
