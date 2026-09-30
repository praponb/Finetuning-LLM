with customer_totals as (
    select customer_id, max(discount) - min(discount) as discount_range
    from orders
    group by customer_id
)
select c.id, c.credit_limit, ct.discount_range
from customers c
inner join customer_totals ct on c.id = ct.customer_id
