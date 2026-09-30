with ranked as (
    select
        id,
        method,
        paid_at,
        order_id,
        row_number() over (partition by method order by paid_at, id) as pos
    from payments
)
select *
from ranked
where pos = 1;
