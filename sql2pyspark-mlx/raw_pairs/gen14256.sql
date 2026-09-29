with latest as (
    -- Keep one row per key (latest first)
    select *, row_number() over (partition by status order by amount desc nulls last, id) as rn
    from orders
    qualify rn = 1
),
matched as (
    -- Non-equi join on a validity window
    select
        src.*,
        sub.id as subscription_id,
        sub.plan,
        sub.monthly_fee
    from latest src
    left join subscriptions sub
        on src.customer_id = sub.customer_id
       and src.order_date >= sub.start_date
       and src.order_date < sub.end_date + interval '7' day
),
kept as (
    -- Semi-join via a correlated subquery
    select s.*
    from matched s
    where exists (
            select 1
            from payments x
            where x.order_id = s.id
              and x.order_id = 1
        )
),
ranked as (
    -- Keep one row per key (latest first)
    select *, rank() over (partition by customer_id order by amount) as rn_2
    from kept
    qualify rn_2 = 1
),
latest_match as (
    -- Latest matching order_item per row (LATERAL ... LIMIT 1)
    select
        s.*,
        latest.quantity,
        latest.unit_price
    from ranked s
    left join lateral (
            select c.quantity, c.unit_price
            from order_items c
            where c.order_id = s.id
            order by c.quantity desc, c.id desc
            limit 1
        ) latest on true
),
latest_match_orders as (
    -- Latest matching shipment per row (LATERAL ... LIMIT 1)
    select
        s.id,
        s.store_id,
        s.status,
        s.channel,
        s.amount,
        s.discount,
        s.rn,
        s.subscription_id,
        s.plan,
        s.monthly_fee,
        s.rn_2,
        top1.shipment_status,
        top1.shipment_delivered_date
    from latest_match s
    left join lateral (
            select c.status as shipment_status, c.delivered_date as shipment_delivered_date
            from shipments c
            where c.order_id = s.id
            order by c.cost desc, c.id desc
            limit 1
        ) top1 on true
),
enriched as (
    -- Running and moving aggregates
    select
        *,
        sum(monthly_fee) over (partition by store_id order by monthly_fee, id, subscription_id rows between unbounded preceding and current row) as cumulative_monthly_fee,
        cume_dist() over (partition by store_id order by monthly_fee nulls first) as monthly_fee_cume_dist
    from latest_match_orders
)
-- Join a dimension and aggregate
select
    p.region,
    p.city,
    src.channel,
    count(*) as cnt,
    avg(src.amount) as avg_amount
from enriched src
inner join stores p on src.store_id = p.id
group by rollup (p.region, p.city, src.channel)
order by city desc
