with latest as (
    -- Deduplicate with ROW_NUMBER
    select
        id,
        order_id,
        delivered_date
    from (
        select *, rank() over (partition by status, carrier order by cost desc) as rn
        from shipments
    ) ranked
    where rn = 1
)
-- Multi-level aggregation
select
    order_id,
    date_trunc('quarter', delivered_date) as quarter_start,
    date_trunc('month', delivered_date) as activity_month,
    count(*) as cnt,
    count(distinct id) as distinct_ids,
    count(*) as num_rows
from latest
group by order_id, date_trunc('quarter', delivered_date), date_trunc('month', delivered_date)
order by distinct_ids desc nulls last
