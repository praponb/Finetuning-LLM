SELECT channel, SUM(amount) AS total_amount, COUNT(*) AS cnt, ROUND(SUM(amount) * 100.0 / SUM(SUM(amount)) OVER (), 2) AS pct_of_total FROM orders GROUP BY channel ORDER BY channel DESC;
