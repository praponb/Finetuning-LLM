select * from (select plan, status, monthly_fee from subscriptions) pivot (count(monthly_fee) for status in ('ACTIVE', 'EXPIRED', 'CANCELLED'));
