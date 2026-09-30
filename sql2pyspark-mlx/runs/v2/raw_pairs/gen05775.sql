select plan, status, count(*) as num_subscriptions from subscriptions where end_date between '2023-04-01' and '2025-01-26' and status = 'PAUSED' group by plan, status
