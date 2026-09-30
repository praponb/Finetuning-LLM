with orders_by_customer as (
    select customer_id, min(order_date) min_order_date
    from orders
    where order_date >= '2024-05-01'
    group by customer_id
)
select customers.name, orders_by_customer.min_order_date
from customers
left join orders_by_customer on customers.id = orders_by_customer.customer_id
order by orders_by_customer.min_order_date desc
