SELECT method, MAX(amount) max_amount
FROM payments
GROUP BY method
