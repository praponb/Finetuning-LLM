SELECT amount, 'payment' AS source FROM payments WHERE status = 'SUCCESS' UNION ALL SELECT monthly_fee, 'subscription' AS source FROM subscriptions WHERE end_date < '2023-03-09' ORDER BY source;
