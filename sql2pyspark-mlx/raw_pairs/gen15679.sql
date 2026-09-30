SELECT status FROM payments UNION SELECT status FROM subscriptions WHERE monthly_fee BETWEEN 100 AND 190 ORDER BY status
