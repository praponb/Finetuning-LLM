with base as (
    select *
    from orders
    where (amount > 710 and channel in ('mobile', 'store')) or store_id in (31, 37)
),
summary as (
    select status, store_id, avg(amount) as avg_amount
    from base
    group by status, store_id
)
select status, store_id, avg_amount
from summary
where avg_amount >= 1090
order by status, store_id
limit 5
