select * from (select plan, status, monthly_fee from subscriptions where start_date >= '2024-02-11') pivot (count(monthly_fee) for status in ('PAUSED', 'CANCELLED', 'EXPIRED')) order by plan
