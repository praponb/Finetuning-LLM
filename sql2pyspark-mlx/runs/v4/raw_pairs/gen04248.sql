select id, monthly_fee, plan, status from subscriptions where monthly_fee > (select max(monthly_fee) from subscriptions)
