with cleaned as (
    -- Filter the raw rows and derive helper columns
    select *, date_sub(delivered_date, 2) as delivered_date_minus_2d
    from shipments
    where delivered_date >= to_date('2023-06-01')
),
enriched as (
    -- Window metrics over several different partitions
    select
        *,
        lag(cost) over w_recent as prev_cost,
        row_number() over w_recent as event_seq
    from cleaned
    window w_recent as (partition by carrier order by cost, id)
),
joined as (
    -- Attach orders attributes
    select
        src.id,
        src.order_id,
        src.cost,
        src.status,
        src.delivered_date_minus_2d,
        src.prev_cost,
        src.event_seq,
        d.amount,
        d.shipping_fee
    from enriched src
    left join orders d
        on src.order_id = d.id
),
cleaned_shipments as (
    -- Filter the raw rows and derive helper columns
    select *, datediff(current_date(), delivered_date_minus_2d) as days_since_delivered_date_minus_2d
    from joined
    where prev_cost between 37 and 45
),
with_dims as (
    -- Attach orders attributes
    select s.*, o.status as order_status
    from cleaned_shipments s
    inner join orders o
        on s.order_id = o.id
)
select
    id,
    cost,
    delivered_date_minus_2d,
    event_seq,
    amount,
    shipping_fee,
    order_status,
    case when order_status rlike '^(PENDING|SHIPPED)$' then 'OTHER_ZONE' else 'GROUP_A' end as order_status_zone
from with_dims
order by order_status_zone;
