select a.id, a.plan, a.monthly_fee from subscriptions a where a.monthly_fee = (select min(b.monthly_fee) from subscriptions b where b.plan = a.plan);
