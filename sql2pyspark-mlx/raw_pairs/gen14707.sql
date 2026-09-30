with base as (
    select *, substring(status, 1, 2) as status_prefix
    from payments
    where paid_at >= timestamp '2023-10-28 18:00:00'
      and order_id is not null
)
select *
from (
    select
        status,
        method,
        amount
    from base
) src
pivot (
    max(amount)
    for method in ('cash', 'card')
)
