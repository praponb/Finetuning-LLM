select * from (select status, channel, discount from orders)
pivot (avg(discount) for channel in ('mobile', 'partner', 'web'));
