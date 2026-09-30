SELECT method, TO_DATE(paid_at) AS paid_day, AVG(amount) AS avg_amount
FROM payments
GROUP BY CUBE(method, TO_DATE(paid_at));
