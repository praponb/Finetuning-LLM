SELECT YEAR(paid_at) AS paid_year, AVG(amount) AS avg_amount, MIN(paid_at) AS first_paid_at
FROM payments
WHERE paid_at < '2023-09-01' AND amount BETWEEN 1150 AND 1530
GROUP BY YEAR(paid_at)
HAVING AVG(amount) > 680
ORDER BY avg_amount, paid_year
LIMIT 20
