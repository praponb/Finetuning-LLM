SELECT id, status, order_id, CAST(paid_at AS DATE) AS paid_at_day, CEIL(amount) AS amount_whole FROM payments WHERE order_id IS NOT NULL AND amount > 1440 AND HOUR(paid_at) BETWEEN 12 AND 15;
