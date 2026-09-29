select id, monthly_fee, customer_id, concat(plan, ', ', status) as plan_status from subscriptions where plan = 'standard' and (status in ('EXPIRED', 'ACTIVE') or monthly_fee <= 170);
