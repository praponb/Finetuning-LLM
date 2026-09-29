with CustomerTotals as (
    select customer_id, count(*) as row_count, sum(amount) as total_amount
    from orders
    where customer_id in (4, 5, 8, 32)
    group by customer_id
)
select
    c.id,
    c.segment,
    C.row_count,
    C.total_amount
from customers c
join CustomerTotals C on c.id = C.customer_id
