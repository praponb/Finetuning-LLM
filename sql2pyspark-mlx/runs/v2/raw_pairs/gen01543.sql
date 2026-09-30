select * from (select status, channel, shipping_fee from orders where amount between 560 and 780)
pivot (sum(shipping_fee) for channel in ('web', 'partner', 'store', 'mobile'));
