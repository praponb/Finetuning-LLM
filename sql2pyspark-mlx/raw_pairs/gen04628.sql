with cleaned as (
    -- Keep recent, valid rows
    select
        id,
        order_id,
        carrier,
        shipped_date,
        delivered_date,
        cost,
        date_trunc('month', delivered_date) delivered_date_period
    from shipments
    where carrier is distinct from 'FedEx'
      and carrier in ('DHL', 'UPS')
),
joined as (
    -- Attach orders attributes
    select
        a.id,
        a.order_id,
        a.shipped_date,
        a.cost,
        a.delivered_date_period,
        p.channel,
        p.amount
    from cleaned a
    inner join orders p
        on a.order_id = p.id
),
filtered as (
    -- Keep recent, valid rows
    select
        *,
        add_months(shipped_date, 6) shipped_date_plus_months,
        coalesce(channel, 'OTHER') resolved_channel
    from joined
    where order_id is not null
),
joined_shipments as (
    -- Attach orders attributes
    select
        s.id,
        s.order_id,
        s.cost,
        s.delivered_date_period,
        s.channel,
        s.amount,
        s.shipped_date_plus_months,
        s.resolved_channel,
        o.channel as order_channel
    from filtered s
    left join orders o
        on s.order_id <=> o.id
)
-- Multi-level aggregation
select
    date_trunc('quarter', shipped_date_plus_months) as quarter_start,
    case when amount >= 1590 then 'LARGE' else 'SMALL' end as amount_tier,
    order_channel,
    min(delivered_date_period) as first_seen,
    count(case when channel = 'store' then 1 end) as store_count,
    (unix_timestamp(max(delivered_date_period)) - unix_timestamp(min(delivered_date_period))) / 60 as span_minutes,
    (unix_timestamp(max(delivered_date_period)) - unix_timestamp(min(delivered_date_period))) / 60 as span_minutes_2
from joined_shipments
group by quarter_start, amount_tier, order_channel
order by quarter_start desc nulls last
