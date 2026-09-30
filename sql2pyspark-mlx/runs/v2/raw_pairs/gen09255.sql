select id, start_date, monthly_fee, customer_id from subscriptions where month(start_date) in (1, 3, 11) and (end_date > '2023-08-01' or start_date >= '2025-05-08') order by id desc;
