select * from (select status, channel, discount from orders)
pivot (max(discount) for channel in ('web', 'mobile'))
