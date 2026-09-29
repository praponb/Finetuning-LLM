select
    id,
    method,
    amount,
    paid_at
from (
    select *, row_number() over (partition by method order by amount desc, id) rn
    from payments
) t
where rn <= 5
