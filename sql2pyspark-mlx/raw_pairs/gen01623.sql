SELECT merchant, SUM(amount) AS total_amount, ROUND(SUM(amount) * 100.0 / SUM(SUM(amount)) OVER (), 2) AS pct_of_total
FROM transactions
GROUP BY merchant
ORDER BY total_amount DESC, pct_of_total ASC, merchant ASC
LIMIT 5
