with cleaned as (
    -- Basic cleansing and defaults
    select
        *,
        date_format(delivered_date, 'MMM yyyy') as delivered_date_label,
        coalesce(carrier, 'DIRECT') as carrier_filled
    from shipments
    where shipped_date >= date '2023-06-28'
),
enriched as (
    -- Attach orders attributes
    select
        e.id,
        e.order_id,
        e.carrier,
        e.shipped_date,
        e.delivered_date,
        b.channel,
        b.discount,
        b.order_date
    from cleaned e
    left join orders b
        on e.order_id <=> b.id
    where b.amount <= 820
),
ranked as (
    -- QUALIFY-style window filter
    select *
    from enriched
    qualify row_number() over (partition by carrier order by shipped_date, id) = 1
),
metrics as (
    -- Running and moving aggregates
    select
        id,
        order_id,
        carrier,
        shipped_date,
        delivered_date,
        channel,
        discount,
        ntile(3) over w_part as discount_bucket,
        lag(discount, 1, 0) over (partition by month(shipped_date), order_id order by discount, id) as prev_discount,
        avg(discount) over (partition by order_id order by discount, id rows between 2 preceding and 1 preceding) as discount_avg_2_prior
    from ranked
    window w_part as (partition by order_id order by discount, id)
),
matched_keys as (
    -- Set difference on keys
    select order_id, carrier
    from metrics
    except all
    select order_id, carrier
    from shipments
    where cost > 61
),
verified as (
    -- Keep rows whose keys survive the set operation
    select s.*
    from metrics s
    join matched_keys k on s.order_id = k.order_id and s.carrier = k.carrier
),
with_dims as (
    -- Attach orders attributes
    select
        s.order_id,
        s.shipped_date,
        s.discount,
        s.discount_bucket,
        s.discount_avg_2_prior,
        p.shipping_fee
    from verified s
    left join orders p
        on s.order_id = p.id
),
with_dims_shipments as (
    -- Attach orders attributes
    select
        s.*,
        p.shipping_fee as order_shipping_fee,
        p.status
    from with_dims s
    join orders p
        on s.order_id = p.id
)
-- Native SQL PIVOT
select *
from (
    select
        order_id,
        status,
        shipping_fee
    from with_dims_shipments
) src
pivot (
    sum(shipping_fee) as amount,
    count(1) as item_count
    for status in ('PENDING' as pending, 'SHIPPED' as shipped)
)
