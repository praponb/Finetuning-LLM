select id, monthly_fee from subscriptions where end_date <= '2024-03-01' and plan in ('premium', 'standard', 'enterprise') order by monthly_fee desc;
