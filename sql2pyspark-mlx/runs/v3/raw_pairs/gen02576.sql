select a.id, a.status, a.monthly_fee, a.customer_id from subscriptions a where a.monthly_fee = (select max(b.monthly_fee) from subscriptions b where b.status = a.status);
