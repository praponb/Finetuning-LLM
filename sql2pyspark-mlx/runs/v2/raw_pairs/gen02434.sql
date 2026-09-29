select end_date, status from subscriptions where monthly_fee < 200 except select end_date, status from subscriptions where start_date between '2024-04-01' and '2025-08-28' order by end_date;
