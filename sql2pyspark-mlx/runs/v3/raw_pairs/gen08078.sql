select status, customer_id, end_date, trim(plan) as clean_plan from subscriptions where status <> 'CANCELLED' and end_date > '2024-07-01' and status in ('PAUSED', 'CANCELLED', 'ACTIVE');
