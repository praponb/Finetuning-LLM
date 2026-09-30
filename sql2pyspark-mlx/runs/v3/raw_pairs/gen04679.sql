select plan from subscriptions where year(end_date) >= 2023 union all select plan from subscriptions where start_date <= '2024-07-01';
