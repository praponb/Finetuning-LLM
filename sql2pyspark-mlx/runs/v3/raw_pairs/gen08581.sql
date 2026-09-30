select s.id, s.plan, s.monthly_fee from subscriptions s where s.monthly_fee > (select avg(s2.monthly_fee) from subscriptions s2 where s2.plan = s.plan)
