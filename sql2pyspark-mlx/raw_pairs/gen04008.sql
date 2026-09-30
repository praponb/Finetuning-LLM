SELECT s.id, s.plan, s.monthly_fee FROM subscriptions s WHERE s.monthly_fee < (SELECT AVG(s2.monthly_fee) FROM subscriptions s2 WHERE s2.plan = s.plan);
