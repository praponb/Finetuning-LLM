select *, discount + amount as discount_plus_amount from orders where channel = 'store' and order_date between '2024-03-01' and '2024-11-01' order by id desc limit 10
