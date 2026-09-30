with valid as (
    -- Keep recent, valid rows
    select
        *,
        concat_ws('/', channel, status) channel_status_key,
        amount - discount amount_minus_discount,
        coalesce(channel, 'NONE') channel_clean
    from orders
    where store_id in (15, 38)
      and channel is not null
),
kept_keys as (
    -- Set difference on keys
    select channel, customer_id
    from valid
    except all
    select channel, customer_id
    from orders
    where status in ('PENDING', 'SHIPPED')
),
kept_rows as (
    -- Keep rows whose keys survive the set operation
    select s.*
    from valid s
    inner join kept_keys k on s.channel = k.channel and s.customer_id = k.customer_id
),
filtered as (
    -- Basic cleansing and defaults
    select
        id,
        customer_id,
        store_id,
        order_date,
        status,
        channel,
        amount,
        discount,
        shipping_fee,
        channel_status_key,
        amount_minus_discount,
        channel_clean,
        shipping_fee - amount amount_with_shipping,
        shipping_fee - amount_minus_discount shipping_fee_minus_amount_minus_discount
    from kept_rows
    where customer_id is not null
      and channel is distinct from 'partner'
),
scored as (
    -- Ranking and gap features
    select
        order_date,
        status,
        amount,
        discount,
        shipping_fee,
        channel_status_key,
        amount_minus_discount,
        channel_clean,
        amount_with_shipping,
        shipping_fee_minus_amount_minus_discount,
        cume_dist() over (order by shipping_fee desc nulls first) shipping_fee_cume_dist,
        corr(shipping_fee, amount) over (partition by status) corr_shipping_fee_amount
    from filtered
    where amount_minus_discount <= 20
),
base as (
    -- Basic cleansing and defaults
    select *, ceil(amount) amount_whole
    from scored
    where discount <= 0.18
),
scored_orders as (
    -- Window metrics over several different partitions
    select
        *,
        dense_rank() over (order by amount_minus_discount desc nulls last) amount_minus_discount_dense_rank,
        min(shipping_fee_cume_dist) over w_ordered order_date_period_min_shipping_fee_cume_dist
    from base
    window w_ordered as (partition by date_trunc('week', order_date))
),
report as (
    select
        status,
        amount,
        channel_status_key,
        amount_minus_discount,
        channel_clean,
        amount_with_shipping,
        shipping_fee_cume_dist,
        amount_whole,
        amount_minus_discount_dense_rank,
        case when status rlike '^(RETURNED|CANCELLED)$' then 'OTHER_ZONE' when status rlike '^(COMPLETED|PENDING)$' then 'GROUP_A' else 'PRIORITY' end as status_zone
    from scored_orders
    order by status_zone desc nulls last, amount_minus_discount_dense_rank desc
)
select
    status,
    amount,
    channel_status_key,
    amount_minus_discount,
    channel_clean,
    amount_with_shipping,
    shipping_fee_cume_dist,
    status_zone,
    case when status_zone rlike '^(PRIORITY|GROUP_A)$' then 'GROUP_B' when status_zone rlike '^(OTHER_ZONE)$' then 'GROUP_A' else 'CORE' end as status_zone_zone
from report;
