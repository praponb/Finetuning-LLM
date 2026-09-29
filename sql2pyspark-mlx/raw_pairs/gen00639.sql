with valid as (
    -- Filter the raw rows and derive helper columns
    select
        *,
        datediff(current_date(), delivered_date) days_since_delivered_date,
        coalesce(carrier, 'N/A') resolved_carrier
    from shipments
    where shipped_date >= to_date('2023-02-24')
      and order_id is not null
),
with_dims as (
    -- Attach orders attributes
    select
        s.id,
        s.order_id,
        s.shipped_date,
        s.cost,
        s.days_since_delivered_date,
        d.order_date
    from valid s
    left join orders d
        on s.order_id = d.id
),
enriched as (
    -- Running and moving aggregates
    select
        *,
        stddev_samp(cost) over (partition by order_id) order_id_stddev_samp_cost,
        round(cost / sum(cost) over (partition by order_id), 4) cost_share
    from with_dims
),
deduped as (
    -- Deduplicate with ROW_NUMBER
    select *, rank() over (partition by order_id order by cost_share desc nulls first) as latest_rank
    from enriched
    qualify latest_rank = 1
)
select
    s.id,
    s.order_id,
    s.shipped_date,
    s.cost,
    s.order_date,
    s.order_id_stddev_samp_cost,
    (select max(x.cost) from shipments x where x.order_id = s.order_id) as max_shipment_cost
from deduped s
order by shipped_date desc;
