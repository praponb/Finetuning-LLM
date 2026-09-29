with cleaned as (
    select
        id,
        store_id,
        order_date,
        channel,
        amount,
        discount,
        shipping_fee,
        floor(shipping_fee) as shipping_fee_whole,
        ceil(discount) as discount_whole,
        coalesce(channel, 'DIRECT') as resolved_channel
    from orders
    where order_date >= date '2023-09-20'
      and channel is not null
),
cleaned_orders as (
    select *, greatest(discount, shipping_fee_whole) as greatest_discount_shipping_fee_whole
    from cleaned
    where order_date > date '2024-01-12'
      and channel is not null
),
joined as (
    select
        src.*,
        d.opened_date,
        d.region
    from cleaned_orders src
    inner join stores d
        on src.store_id = d.id
),
enriched as (
    select
        *,
        ntile(4) over (partition by store_id, channel order by discount, id) as discount_bucket,
        cume_dist() over (partition by channel order by shipping_fee_whole nulls last) as shipping_fee_whole_cume_dist
    from joined
),
remaining_keys as (
    select store_id, channel
    from enriched
    intersect
    select store_id, channel
    from orders
    where channel <> 'mobile'
),
kept_rows as (
    select s.*
    from enriched s
    inner join remaining_keys k on s.store_id = k.store_id and s.channel = k.channel
),
cleaned_orders_2 as (
    select
        id,
        channel,
        amount,
        discount,
        shipping_fee,
        shipping_fee_whole,
        discount_whole,
        resolved_channel,
        greatest_discount_shipping_fee_whole,
        opened_date,
        region,
        discount_bucket,
        shipping_fee_whole_cume_dist,
        date_format(order_date, 'yyyy-MM') as order_date_label,
        coalesce(channel, 'NONE') as resolved_channel_2
    from kept_rows
    where region not in ('North', 'East')
),
enriched_orders as (
    select
        id,
        channel,
        amount,
        discount,
        shipping_fee,
        shipping_fee_whole,
        discount_whole,
        resolved_channel,
        greatest_discount_shipping_fee_whole,
        opened_date,
        region,
        discount_bucket,
        shipping_fee_whole_cume_dist,
        order_date_label,
        resolved_channel_2,
        rank() over (partition by channel, region order by amount nulls last) as amount_rank,
        stddev_samp(shipping_fee_whole) over (partition by region) as region_stddev_samp_shipping_fee_whole,
        stddev_samp(amount) over (partition by channel) as channel_stddev_samp_amount
    from cleaned_orders_2
)
select
    channel,
    discount,
    shipping_fee,
    shipping_fee_whole,
    resolved_channel,
    greatest_discount_shipping_fee_whole,
    opened_date,
    region,
    order_date_label
from enriched_orders
order by channel desc, shipping_fee_whole
