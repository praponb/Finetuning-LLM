select * from (select plan, status, monthly_fee from subscriptions where plan != 'enterprise') pivot (avg(monthly_fee) for status in ('ACTIVE', 'PAUSED', 'EXPIRED')) order by plan
