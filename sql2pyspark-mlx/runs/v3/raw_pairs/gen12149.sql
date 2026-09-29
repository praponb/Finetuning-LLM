select id, end_date, case when status = 'EXPIRED' then 1 else 0 end as is_expired from subscriptions where start_date >= '2023-09-01' and monthly_fee between 90 and 150
