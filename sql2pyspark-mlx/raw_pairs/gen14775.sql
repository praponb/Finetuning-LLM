select plan, start_date, status, date_add(end_date, 3) as end_date_plus_3d from subscriptions where month(end_date) in (2, 6, 9) and (customer_id is not null or start_date < '2024-04-01')
