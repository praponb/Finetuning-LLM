select customer_id, num_orders, avg_discount
from (
    select customer_id, count(*) as num_orders, avg(discount) as avg_discount
    from orders
    group by customer_id
) as t
where avg_discount < 0.27;
