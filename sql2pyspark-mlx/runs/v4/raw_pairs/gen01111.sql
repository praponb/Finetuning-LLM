select * from (select channel, status, shipping_fee from orders where amount < 1480)
pivot (max(shipping_fee) for status in ('PENDING', 'RETURNED', 'CANCELLED', 'COMPLETED'))
