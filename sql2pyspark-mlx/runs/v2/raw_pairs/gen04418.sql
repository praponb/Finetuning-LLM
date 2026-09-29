SELECT s.id, s.plan, s.monthly_fee, s.status FROM subscriptions s WHERE s.monthly_fee = (SELECT MAX(s2.monthly_fee) FROM subscriptions s2 WHERE s2.plan = s.plan) ORDER BY s.plan
