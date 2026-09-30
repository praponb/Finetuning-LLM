select a.id, a.status, a.monthly_fee from subscriptions a where a.monthly_fee = (select min(b.monthly_fee) from subscriptions b where b.status = a.status) order by a.monthly_fee;
