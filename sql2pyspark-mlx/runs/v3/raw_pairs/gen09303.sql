SELECT YEAR(paid_at) AS paid_year, method, ROUND(AVG(amount), 2) AS avg_amount
FROM payments
WHERE amount <= 1030 AND paid_at >= '2024-11-01' AND method = 'card'
GROUP BY ROLLUP(YEAR(paid_at), method);
