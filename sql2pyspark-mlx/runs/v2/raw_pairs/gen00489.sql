SELECT id, start_date, end_date FROM subscriptions WHERE status <> 'EXPIRED' EXCEPT SELECT id, start_date, end_date FROM subscriptions WHERE monthly_fee BETWEEN 80 AND 120;
