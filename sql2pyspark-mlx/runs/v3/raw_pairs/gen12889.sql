with ranked as (
    select
        id,
        method,
        amount,
        row_number() over (partition by method order by amount desc, id) as rn
    from payments
)
select id, method, amount
from ranked
where rn <= 5
order by amount desc
