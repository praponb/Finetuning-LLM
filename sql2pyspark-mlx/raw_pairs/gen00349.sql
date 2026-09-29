select * from (select plan, status, monthly_fee from subscriptions) src pivot (sum(monthly_fee) as amount, count(1) as item_count for status in ('ACTIVE' as active, 'CANCELLED' as cancelled));
