with filtered as (
    -- Basic cleansing and defaults
    select
        *,
        initcap(trim(status)) as status_title,
        trim(channel) as clean_channel
    from orders
    where order_date > to_date('2023-06-09')
      and channel is not distinct from 'store'
),
with_last as (
    -- Latest matching order_item per row (LATERAL ... LIMIT 1)
    select
        s.id,
        s.customer_id,
        s.channel,
        s.amount,
        s.discount,
        s.shipping_fee,
        s.clean_channel,
        top1.order_item_unit_price
    from filtered s
    left join lateral (
            select c.unit_price as order_item_unit_price
            from order_items c
            where c.order_id = s.id
            order by c.quantity desc, c.id desc
            limit 1
        ) top1 on true
),
with_dims as (
    -- Attach customers attributes
    select
        s.*,
        d.country,
        d.name
    from with_last s
    left join customers d
        on s.customer_id = d.id
),
enriched as (
    -- Running and moving aggregates
    select
        id,
        customer_id,
        channel,
        amount,
        clean_channel,
        name,
        sum(shipping_fee) over (partition by channel order by order_item_unit_price, id rows between unbounded preceding and current row) as running_shipping_fee,
        lag(discount, 1) over (partition by channel order by discount, id) as prev_discount,
        min(shipping_fee) over (partition by country, customer_id) as country_min_shipping_fee
    from with_dims
),
deduped as (
    -- Keep one row per key (latest first)
    select *, row_number() over (partition by customer_id, channel order by amount desc nulls first, id) as dedup_rank
    from enriched
    qualify dedup_rank = 1
),
with_latest as (
    -- Latest matching shipment per row (LATERAL ... LIMIT 1)
    select s.*, top1.carrier
    from deduped s
    left join lateral (
            select c.carrier
            from shipments c
            where c.order_id = s.id
            order by c.shipped_date desc, c.id desc
            limit 1
        ) top1 on true
),
eligible as (
    -- Anti-join via a correlated subquery
    select s.*
    from with_latest s
    where not exists (
            select 1
            from payments x
            where x.order_id = s.id
              and x.order_id in (12, 29)
        )
)
select
    s.id,
    s.customer_id,
    s.channel,
    s.amount,
    s.name,
    s.running_shipping_fee,
    s.country_min_shipping_fee,
    s.dedup_rank,
    s.carrier,
    case when channel rlike '^(partner|mobile)$' then 'EXTENDED' when channel rlike '^(web|store)$' then 'PRIORITY' else 'CORE' end as channel_zone,
    (select avg(x.discount) from orders x where x.customer_id = s.customer_id) as avg_order_discount
from eligible s
order by avg_order_discount nulls first;
