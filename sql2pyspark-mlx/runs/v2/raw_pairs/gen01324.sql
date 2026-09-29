select id, plan, start_date from subscriptions where end_date <= '2024-11-13' union all select id, plan, start_date from subscriptions where status = 'PAUSED';
