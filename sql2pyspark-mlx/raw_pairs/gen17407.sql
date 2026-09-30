with valid as (
    -- Basic cleansing and defaults
    select *, ceil(amount) as amount_whole
    from payments
    where order_id is not null
      and method != 'card'
),
deduped as (
    -- Keep one row per key (latest first)
    select
        id,
        order_id,
        amount,
        status,
        amount_whole
    from (
        select *, row_number() over (partition by method, status order by amount asc, id asc) as dedup_rank
        from valid
    ) x
    where dedup_rank = 1
),
deduped_payments as (
    -- Keep one row per key (latest first)
    select *
    from deduped
    qualify row_number() over (partition by status order by amount_whole desc, id asc) = 1
),
enriched as (
    -- Attach orders attributes
    select src.*, o.order_date
    from deduped_payments src
    join orders o
        on src.order_id = o.id
    where o.shipping_fee between 13 and 21
),
summary as (
    -- Grouped metrics
    select
        case when amount_whole >= 83 then 'LARGE' else 'SMALL' end as amount_whole_tier,
        order_id,
        round(sum(amount) / nullif(count(case when status = 'FAILED' then 1 end), 0), 2) as amount_per_failed,
        count(distinct id) as distinct_ids,
        count(*) as event_count,
        grouping(case when amount_whole >= 83 then 'LARGE' else 'SMALL' end) as grouping_level
    from enriched
    group by grouping sets ((case when amount_whole >= 83 then 'LARGE' else 'SMALL' end, order_id), (case when amount_whole >= 83 then 'LARGE' else 'SMALL' end))
)
select
    s.amount_whole_tier,
    s.order_id,
    s.amount_per_failed,
    s.distinct_ids,
    s.event_count,
    s.grouping_level,
    (select min(t2.cost) from shipments t2 where t2.order_id = s.order_id) as min_shipment_cost
from summary s
order by min_shipment_cost desc nulls last
