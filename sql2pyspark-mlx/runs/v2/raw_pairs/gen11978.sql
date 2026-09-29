SELECT a.id, a.status, a.monthly_fee FROM subscriptions a WHERE a.monthly_fee = (SELECT MIN(b.monthly_fee) FROM subscriptions b WHERE b.status = a.status) ORDER BY a.monthly_fee DESC;
