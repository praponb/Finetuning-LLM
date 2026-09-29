SELECT status, COUNT(*) AS row_count, MIN(paid_at) AS first_paid_at FROM payments WHERE amount >= 640 AND order_id = 10 AND amount BETWEEN 670 AND 1240 GROUP BY status
