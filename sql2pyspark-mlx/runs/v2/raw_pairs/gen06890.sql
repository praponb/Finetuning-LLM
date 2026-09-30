SELECT COUNT(amount) AS amount_count, ROUND(AVG(amount), 2) AS avg_amount FROM payments WHERE order_id IS NOT NULL AND amount < 500
