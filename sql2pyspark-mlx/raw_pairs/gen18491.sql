with valid as (
    -- Basic cleansing and defaults
    select
        *,
        coalesce(cost, 0) as cost_filled,
        coalesce(carrier, 'UNKNOWN') as resolved_carrier
    from shipments
    where shipped_date >= date '2023-04-21'
),
enriched as (
    -- Window metrics over several different partitions
    select
        *,
        lead(cost) over w_running as next_cost,
        lead(cost_filled) over w_running as next_cost_filled,
        lag(cost_filled) over w_running as prev_cost_filled,
        row_number() over w_running as seq_no
    from valid
    where carrier = 'Kerry'
    window w_running as (partition by carrier, order_id order by cost, id)
),
joined as (
    -- Attach orders attributes
    select
        a.id,
        a.order_id,
        a.shipped_date,
        a.delivered_date,
        a.cost,
        a.resolved_carrier,
        a.next_cost,
        a.next_cost_filled,
        a.seq_no,
        d.order_date,
        d.status as order_status,
        d.shipping_fee
    from enriched a
    inner join orders d
        on a.order_id = d.id
),
valid_shipments as (
    -- Basic cleansing and defaults
    select
        id,
        shipped_date,
        delivered_date,
        next_cost_filled,
        seq_no,
        order_date,
        shipping_fee,
        date_sub(shipped_date, 44) as shipped_date_minus_44d,
        coalesce(order_status, 'UNKNOWN') as order_status_filled
    from joined
    where order_status is distinct from 'PENDING'
),
cleaned as (
    -- Filter the raw rows and derive helper columns
    select
        id,
        shipped_date,
        delivered_date,
        next_cost_filled,
        seq_no,
        order_date,
        shipping_fee,
        order_status_filled,
        round(datediff(delivered_date, order_date) / 30, 1) as months_order_date_to_delivered_date
    from valid_shipments
    where next_cost_filled > 33
)
select
    id,
    shipped_date,
    next_cost_filled,
    order_date,
    shipping_fee,
    order_status_filled,
    months_order_date_to_delivered_date
from cleaned;
