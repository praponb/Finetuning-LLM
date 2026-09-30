SELECT status, COUNT(amount) AS amount_count, AVG(amount) AS avg_amount FROM payments WHERE method = 'promptpay' AND status IN ('SUCCESS', 'REFUNDED', 'FAILED') GROUP BY status
