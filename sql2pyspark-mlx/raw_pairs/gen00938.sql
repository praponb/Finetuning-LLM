SELECT id, monthly_fee FROM subscriptions WHERE monthly_fee > (SELECT MAX(monthly_fee) FROM subscriptions WHERE status != 'ACTIVE') ORDER BY monthly_fee DESC, id ASC LIMIT 10;
