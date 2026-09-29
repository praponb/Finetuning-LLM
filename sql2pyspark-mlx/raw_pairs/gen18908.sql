with base as (
    -- Keep recent, valid rows
    select
        *,
        datediff(delivered_date, shipped_date) / 7 as weeks_shipped_date_to_delivered_date,
        coalesce(carrier, 'UNKNOWN') as carrier_clean
    from shipments
    where order_id is not null
),
enriched as (
    -- Attach orders attributes
    select
        a.id,
        a.order_id,
        a.carrier,
        a.delivered_date,
        a.status,
        a.weeks_shipped_date_to_delivered_date,
        d.shipping_fee,
        d.discount,
        d.amount
    from base a
    left join orders d
        on a.order_id = d.id
),
metrics as (
    -- Window metrics over several different partitions
    select
        id,
        order_id,
        delivered_date,
        discount,
        avg(shipping_fee) over (partition by status order by discount, id rows between 3 preceding and current row) as last4_avg_shipping_fee,
        min(shipping_fee) over (partition by status order by discount, id rows between 3 preceding and current row) as last4_min_shipping_fee,
        max(shipping_fee) over (partition by status order by discount, id rows between 3 preceding and current row) as last4_max_shipping_fee,
        lead(amount, 1) over (order by delivered_date, id) as next_amount
    from enriched
),
filtered as (
    -- Filter the raw rows and derive helper columns
    select *, ceil(next_amount) as next_amount_whole
    from metrics
    where delivered_date > date '2023-04-03'
      and order_id is not null
)
-- Native SQL UNPIVOT (wide to long)
select
    id,
    order_id,
    metric_name,
    reading
from (
    select
        id,
        order_id,
        last4_min_shipping_fee,
        next_amount,
        last4_max_shipping_fee
    from filtered
) src
unpivot include nulls (
    reading for metric_name in (last4_min_shipping_fee, next_amount, last4_max_shipping_fee)
);
