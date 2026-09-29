SELECT method, SUM(CASE WHEN method = 'card' THEN amount ELSE 0 END) AS card_amount, SUM(CASE WHEN status = 'REFUNDED' THEN 1 ELSE 0 END) AS refunded_count FROM payments GROUP BY method
