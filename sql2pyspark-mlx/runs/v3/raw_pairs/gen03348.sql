select a.id, a.plan, a.monthly_fee from subscriptions a where a.monthly_fee > (select avg(b.monthly_fee) from subscriptions b where b.plan = a.plan) order by a.plan desc, a.monthly_fee desc
