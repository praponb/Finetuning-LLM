SELECT status, MAX(amount) - MIN(amount) AS amount_range, MIN(paid_at) AS first_paid_at
FROM payments
GROUP BY 1
HAVING MIN(amount) > 810
ORDER BY status ASC
