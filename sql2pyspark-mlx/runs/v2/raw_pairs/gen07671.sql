select customer_id, country from sessions where to_date(started_at) >= '2023-08-01' union select customer_id, country from sessions where customer_id in (7, 12, 20, 31)
