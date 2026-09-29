select * from (select status, channel, amount from orders)
pivot (count(amount) for channel in ('web', 'mobile', 'partner', 'store'));
