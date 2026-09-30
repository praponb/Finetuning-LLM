with ranked as (
    select *, dense_rank() over (partition by status order by discount desc) as rnk
    from orders
)
select
    id,
    status,
    discount,
    channel
from ranked
where rnk <= 5;
