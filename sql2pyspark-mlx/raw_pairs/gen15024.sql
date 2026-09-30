with rollup_totals as (
    select
        c.country,
        a.status,
        count(*) as num_rows,
        max(a.monthly_fee) as max_monthly_fee,
        grouping_id() as gid
    from subscriptions a
    left join customers c on a.customer_id = c.id
    group by rollup (c.country, a.status)
)
select
    status,
    max_monthly_fee,
    gid
from rollup_totals;
