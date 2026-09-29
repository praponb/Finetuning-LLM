SELECT method, SUM(amount) AS total_amount, ROUND(SUM(amount) * 100.0 / SUM(SUM(amount)) OVER (), 2) AS pct_of_total FROM payments WHERE order_id = 6 GROUP BY method ORDER BY pct_of_total DESC;
