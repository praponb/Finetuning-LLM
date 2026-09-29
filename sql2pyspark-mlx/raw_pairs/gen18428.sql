with dim_totals as (
    select
        o.status as order_status,
        o.channel,
        a.carrier,
        count(*) as rows_total,
        min(a.cost) as min_cost
    from shipments a
    left join orders o on a.order_id = o.id
    group by grouping sets ((o.status, o.channel, a.carrier), (o.status))
)
select
    order_status,
    carrier,
    min_cost
from dim_totals
order by order_status desc, min_cost
