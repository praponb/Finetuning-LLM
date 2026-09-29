SELECT status, 'payment' source FROM payments WHERE amount >= 1210 UNION SELECT status, 'subscription' source FROM subscriptions WHERE MONTH(end_date) IN (4, 5, 12) ORDER BY status DESC
