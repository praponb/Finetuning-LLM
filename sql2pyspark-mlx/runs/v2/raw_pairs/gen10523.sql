SELECT start_date FROM subscriptions WHERE status = 'EXPIRED' UNION ALL SELECT start_date FROM subscriptions WHERE monthly_fee > 190 ORDER BY start_date DESC
