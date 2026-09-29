SELECT method, QUARTER(paid_at) AS paid_quarter, SUM(amount) AS total_amount
FROM payments
WHERE amount BETWEEN 1240 AND 1260
GROUP BY method, QUARTER(paid_at);
