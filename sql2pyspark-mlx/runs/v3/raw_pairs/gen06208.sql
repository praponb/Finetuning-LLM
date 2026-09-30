with ranked as (
    select
        id,
        status,
        order_date,
        row_number() over (partition by status order by order_date, id) as rnk
    from orders
)
select id, status, order_date
from ranked
where rnk = 1
