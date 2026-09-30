select channel, status, count(shipping_fee) as shipping_fee_count from orders group by channel, status order by status asc
