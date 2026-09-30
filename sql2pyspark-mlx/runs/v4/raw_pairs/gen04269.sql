select * from (select plan, status, monthly_fee from subscriptions) pivot (max(monthly_fee) for status in ('PAUSED', 'ACTIVE', 'EXPIRED', 'CANCELLED')) order by plan
