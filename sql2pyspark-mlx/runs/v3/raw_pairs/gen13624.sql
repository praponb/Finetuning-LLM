SELECT a.id, a.plan, a.monthly_fee FROM subscriptions a WHERE a.monthly_fee = (SELECT MAX(b.monthly_fee) FROM subscriptions b WHERE b.plan = a.plan);
