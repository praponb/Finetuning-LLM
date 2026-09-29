select id, start_date, plan, monthly_fee, lower(plan) as plan_lower from subscriptions where plan in ('enterprise', 'basic') and plan = 'premium' and plan <> 'enterprise'
