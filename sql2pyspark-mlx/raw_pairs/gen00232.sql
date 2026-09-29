select
    o.id,
    o.customer_id,
    o.order_date,
    o.amount
from orders o
where o.customer_id not in (select id from customers where age <> 39)
order by amount desc
