SELECT order_id, status, MAX(paid_at) AS latest_paid_at, SUM(CASE WHEN method = 'card' THEN 1 ELSE 0 END) AS card_count FROM payments GROUP BY 1, 2;
