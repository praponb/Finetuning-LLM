with RecentPayments as (
    select *
    from payments
    where order_id = 17
),
agg as (
    select method, max(amount) - min(amount) as amount_range
    from RecentPayments
    group by method
)
select method, amount_range
from agg
where amount_range > 35
order by method desc, amount_range desc;
