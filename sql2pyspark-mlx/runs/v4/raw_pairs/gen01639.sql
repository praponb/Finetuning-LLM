with base as (
    -- Keep recent, valid rows
    select *, substring(carrier, 1, 3) as carrier_prefix
    from shipments
    where order_id is not null
),
enriched as (
    -- Attach orders attributes
    select
        a.*,
        o.status as order_status,
        o.shipping_fee
    from base a
    left join orders o
        on a.order_id = o.id
),
latest as (
    -- QUALIFY-style window filter
    select *
    from (
        select *, rank() over (partition by status order by delivered_date desc) as dedup_rank
        from enriched
    ) ranked
    where dedup_rank = 1
),
scored as (
    -- Ranking and gap features
    select
        id,
        order_id,
        carrier,
        shipped_date,
        delivered_date,
        cost,
        status,
        carrier_prefix,
        order_status,
        shipping_fee,
        dedup_rank,
        percent_rank() over (partition by order_status order by shipping_fee desc) as shipping_fee_percent_rank,
        rank() over (partition by status order by cost desc) as cost_rank,
        percent_rank() over w_recent as shipping_fee_percent_rank_2
    from latest
    window w_recent as (partition by status order by shipping_fee desc nulls last)
),
with_dims as (
    -- Attach orders attributes
    select s.*, p.channel
    from scored s
    left join orders p
        on s.order_id = p.id
)
-- Aggregate with conditional metrics
select
    date_trunc('quarter', delivered_date) as quarter_start,
    sort_array(collect_set(order_status)) as order_status_list,
    round(avg(shipping_fee), 2) as avg_shipping_fee,
    sum(shipping_fee_percent_rank) filter (where status = 'IN_TRANSIT') as in_transit_total_shipping_fee_percent_rank,
    count_if(status = 'LOST') as lost_events,
    count_if(channel = 'store') as store_events
from with_dims
group by 1
having avg_shipping_fee > 0
order by in_transit_total_shipping_fee_percent_rank
