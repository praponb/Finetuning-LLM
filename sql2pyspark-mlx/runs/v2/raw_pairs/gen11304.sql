with ranked as (
    select *, rank() over (partition by status order by amount desc) as pos
    from payments
    where status != 'REFUNDED'
)
select
    id,
    status,
    amount,
    method
from ranked
where pos <= 5;
