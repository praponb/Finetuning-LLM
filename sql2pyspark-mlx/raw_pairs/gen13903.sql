SELECT id, status, paid_at, amount, ROUND(AVG(amount) OVER (PARTITION BY status ORDER BY paid_at, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW), 2) AS amount_moving_avg_7 FROM payments;
