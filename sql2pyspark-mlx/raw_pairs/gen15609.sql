with numbered as (
    select *, rank() over (partition by customer_id order by amount desc) as rnk
    from orders
)
select id, customer_id, amount
from numbered
where rnk = 1
order by customer_id asc;
