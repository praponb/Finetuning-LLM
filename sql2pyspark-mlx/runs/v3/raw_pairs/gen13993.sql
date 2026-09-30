with top_customers as (
    select *, rank() over (partition by segment order by credit_limit desc) as rn
    from customers
)
select
    id,
    segment,
    credit_limit,
    is_active
from top_customers
where rn <= 3
