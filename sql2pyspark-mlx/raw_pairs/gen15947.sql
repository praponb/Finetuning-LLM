select a.id, a.status, a.monthly_fee from subscriptions a where a.monthly_fee < (select avg(b.monthly_fee) from subscriptions b where b.status = a.status)
