SELECT end_date, status FROM subscriptions WHERE monthly_fee >= 170 INTERSECT SELECT end_date, status FROM subscriptions WHERE YEAR(start_date) >= 2025;
