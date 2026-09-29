with enriched as (
    -- Attach orders attributes
    select
        s.id,
        s.order_id,
        s.method,
        s.amount,
        s.status,
        p.amount as order_amount,
        p.shipping_fee,
        p.order_date
    from payments s
    left join orders p
        on s.order_id <=> p.id
),
joined as (
    -- Attach orders attributes
    select
        e.id,
        e.order_id,
        e.amount,
        e.order_amount,
        e.shipping_fee,
        d.amount as order_amount_2,
        d.status as order_status,
        d.order_date as order_order_date
    from enriched e
    left join orders d
        on e.order_id <=> d.id
),
with_dims as (
    -- Attach orders attributes
    select
        a.*,
        b.channel,
        b.order_date,
        b.shipping_fee as order_shipping_fee
    from joined a
    left join orders b
        on a.order_id <=> b.id
),
latest as (
    -- QUALIFY-style window filter
    select
        id,
        order_id,
        amount,
        shipping_fee,
        order_amount_2,
        order_status,
        order_date
    from (
        select *, row_number() over (partition by channel, order_id order by order_date desc nulls first, id asc) as latest_rank
        from with_dims
    ) x
    where latest_rank = 1
),
metrics as (
    -- Running and moving aggregates
    select
        *,
        lag(amount) over w_hist prev_amount,
        lead(order_amount_2) over w_hist next_order_amount_2,
        row_number() over w_hist row_seq
    from latest
    window w_hist as (partition by order_id order by amount asc, id asc)
)
select
    id,
    amount,
    order_amount_2,
    order_status,
    order_date,
    prev_amount,
    next_order_amount_2,
    row_seq
from metrics
order by order_date desc nulls first;
