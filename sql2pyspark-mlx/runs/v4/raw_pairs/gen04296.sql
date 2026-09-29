with ranked as (
    -- Keep one row per key (latest first)
    select *
    from (
        select *, row_number() over (partition by status order by shipped_date desc, id) as rank_in_group
        from shipments
    ) t
    where rank_in_group = 1
),
enriched as (
    -- Attach orders attributes
    select
        a.*,
        d.status as order_status,
        d.channel
    from ranked a
    left join orders d
        on a.order_id = d.id
),
cleaned as (
    -- Keep recent, valid rows
    select *, coalesce(cost, 0) as cost_filled
    from enriched
    where shipped_date > date '2023-09-13'
      and order_id is not null
)
-- Grouped metrics
select
    channel,
    count(case when order_status = 'CANCELLED' then 1 end) as cancelled_count,
    min(cost_filled) as min_cost_filled
from cleaned
group by channel
order by min_cost_filled desc nulls last;
