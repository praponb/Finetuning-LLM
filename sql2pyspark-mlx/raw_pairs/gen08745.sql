with cleaned as (
    -- Keep recent, valid rows
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
        year(order_date) as order_date_year,
        month(order_date) as order_date_month
    from orders
    where store_id is not null
),
cleaned_orders as (
    -- Filter the raw rows and derive helper columns
    select
        *,
        round(order_date_month, 2) as order_date_month_rounded,
        order_date_month / nullif(discount, 0) as order_date_month_per_discount
    from cleaned
    where order_date >= to_date('2023-08-21')
      and channel is not null
),
ranked as (
    -- QUALIFY-style window filter
    select
        id,
        store_id,
        status,
        order_date_month_rounded,
        order_date_month_per_discount
    from cleaned_orders
    qualify row_number() over (partition by customer_id, channel order by order_date_month_rounded desc, id) = 1
),
with_latest as (
    -- Latest matching order_item per row (LATERAL ... LIMIT 1)
    select s.*, top1.unit_price
    from ranked s
    left join lateral (
            select c.unit_price
            from order_items c
            where c.order_id = s.id
            order by c.unit_price desc, c.id desc
            limit 1
        ) top1 on true
),
deduped as (
    -- Keep one row per key (latest first)
    select *, row_number() over (partition by status order by order_date_month_per_discount desc, id) as latest_rank
    from with_latest
    qualify latest_rank = 1
),
metrics as (
    -- Running and moving aggregates
    select
        *,
        lag(order_date_month_rounded) over w_hist as prev_order_date_month_rounded,
        lead(order_date_month_rounded) over w_hist as next_order_date_month_rounded,
        lag(order_date_month_per_discount) over w_hist as prev_order_date_month_per_discount,
        lead(order_date_month_per_discount) over w_hist as next_order_date_month_per_discount,
        row_number() over w_hist as seq_no,
        lead(unit_price, 1) over w_recent as next_unit_price
    from deduped
    window w_hist as (partition by store_id order by order_date_month_rounded, id), w_recent as (partition by status order by unit_price, id)
),
valid as (
    -- Filter the raw rows and derive helper columns
    select
        *,
        round(next_unit_price * 0.9, 2) as next_unit_price_adjusted,
        floor(unit_price) as unit_price_whole
    from metrics
    where store_id is not null
      and prev_order_date_month_rounded > 64
),
by_type as (
    -- Native SQL PIVOT
    select *
    from (
        select
            store_id,
            status,
            prev_order_date_month_rounded
        from valid
    ) src
    pivot (
        sum(prev_order_date_month_rounded) as value_sum,
        count(1) as cnt
        for status in ('PENDING' as pending, 'CANCELLED' as cancelled)
    )
)
select
    store_id,
    pending_value_sum,
    pending_cnt,
    cancelled_value_sum,
    cancelled_cnt
from by_type
order by cancelled_value_sum desc nulls last;
