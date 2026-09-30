SELECT id, monthly_fee, status, end_date FROM subscriptions WHERE monthly_fee > (SELECT AVG(monthly_fee) FROM subscriptions) ORDER BY 3, end_date;
