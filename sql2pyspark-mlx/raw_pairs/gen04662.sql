SELECT status, SUM(amount) AS total_amount, COUNT(*) AS cnt, SUM(SUM(amount)) OVER (ORDER BY status) AS cumulative_amount FROM payments WHERE order_id = 33 GROUP BY status ORDER BY total_amount DESC;
