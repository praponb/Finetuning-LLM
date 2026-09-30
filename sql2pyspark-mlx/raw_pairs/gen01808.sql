select id, customer_id, status from subscriptions where plan in ('enterprise', 'standard', 'basic') union all select id, customer_id, status from subscriptions where monthly_fee > 190
