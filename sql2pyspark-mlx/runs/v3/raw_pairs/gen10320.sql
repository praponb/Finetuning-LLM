with ranked as (
    select *, row_number() over (partition by status order by amount, id) as pos
    from payments
)
select
    id,
    status,
    amount,
    method
from ranked
where pos <= 3
