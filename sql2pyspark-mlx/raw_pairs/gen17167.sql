select * from (select id, channel, amount, shipping_fee from orders) src unpivot (reading for metric_name in (amount as AMOUNT, shipping_fee as SHIPPING_FEE))
