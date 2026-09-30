SELECT id, start_date, end_date FROM subscriptions WHERE monthly_fee BETWEEN 100 AND 160 INTERSECT SELECT id, start_date, end_date FROM subscriptions WHERE status = 'EXPIRED';
