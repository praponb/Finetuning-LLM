SELECT status, SUM(amount) AS total_amount, ROUND(SUM(amount) * 100.0 / SUM(SUM(amount)) OVER (), 2) AS pct_of_total
FROM payments
GROUP BY status;
