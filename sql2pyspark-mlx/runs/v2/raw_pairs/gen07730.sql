SELECT method, MIN(amount) AS min_amount, COUNT(CASE WHEN method = 'paypal' THEN 1 END) AS paypal_count
FROM payments
WHERE method = 'paypal' AND TO_DATE(paid_at) < '2024-03-01' AND status NOT IN ('REFUNDED', 'FAILED')
GROUP BY method
HAVING min_amount > 1100
