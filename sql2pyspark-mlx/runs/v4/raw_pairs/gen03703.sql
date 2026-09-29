select * from (select plan, customer_id, status, monthly_fee from subscriptions) src pivot (avg(monthly_fee) for status in ('EXPIRED' as expired, 'PAUSED' as paused, 'CANCELLED' as cancelled))
