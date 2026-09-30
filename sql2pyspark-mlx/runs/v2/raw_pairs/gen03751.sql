select
    customers.id,
    customers.country,
    count(orders.id) as order_count,
    max(orders.order_date) as latest_order_date,
    count(orders.shipping_fee) as shipping_fee_count
from customers
left outer join orders on customers.id = orders.customer_id
group by customers.id, customers.country
having count(orders.id) >= 3
order by id
