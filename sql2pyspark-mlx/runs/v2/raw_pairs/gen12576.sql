with Ranked as (
    select
        id,
        order_id,
        paid_at,
        row_number() over (partition by order_id order by paid_at desc, id) as pos
    from payments
    where to_date(paid_at) < '2023-05-01'
)
select *
from Ranked
where pos <= 3
order by id desc, paid_at
