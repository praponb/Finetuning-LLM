SELECT status, 'payment' AS source FROM payments WHERE order_id IN (12, 13) UNION SELECT status, 'subscription' AS source FROM subscriptions WHERE monthly_fee >= 210 ORDER BY source DESC
