SELECT SUM(amount) total_amount, ROUND(AVG(amount), 2) avg_amount FROM orders WHERE shipping_fee > 27 AND status = 'RETURNED' ORDER BY avg_amount DESC;
