select id, start_date, plan, status, case when plan in ('enterprise', 'premium') then 'Core' else 'Other' end as plan_group from subscriptions where monthly_fee >= 200;
