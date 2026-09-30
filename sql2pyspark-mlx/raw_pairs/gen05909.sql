SELECT s.id, s.status, s.monthly_fee FROM subscriptions s WHERE s.monthly_fee > (SELECT AVG(s2.monthly_fee) FROM subscriptions s2 WHERE s2.status = s.status)
