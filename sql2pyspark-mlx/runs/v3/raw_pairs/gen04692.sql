select
    id,
    shipping_fee,
    order_date,
    status
from orders
where shipping_fee = (select min(shipping_fee) from orders)
order by order_date desc, id
limit 5
