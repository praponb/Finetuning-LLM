with filtered as (
    -- Filter the raw rows and derive helper columns
    select *, concat_ws('-', status, channel) status_channel_key
    from orders
    where order_date > date '2023-09-26'
),
metrics as (
    -- Window metrics over several different partitions
    select
        id,
        customer_id,
        order_date,
        amount,
        discount,
        shipping_fee,
        status_channel_key,
        sum(discount) over (partition by month(order_date), status order by amount asc, id asc rows between unbounded preceding and current row) cumulative_discount,
        row_number() over (partition by customer_id order by shipping_fee asc, id asc) seq_no,
        ntile(10) over (partition by month(order_date), channel order by shipping_fee desc, id asc) shipping_fee_bucket,
        row_number() over (partition by date_trunc('YEAR', order_date), channel order by amount asc, id asc) row_seq
    from filtered
    where channel = 'web'
),
valid as (
    -- Filter the raw rows and derive helper columns
    select *, round(discount * 1.15, 2) adjusted_discount
    from metrics
    where shipping_fee <= 34
),
latest_match as (
    -- Latest matching order_item per row (LATERAL ... LIMIT 1)
    select
        s.id,
        s.order_date,
        s.amount,
        s.discount,
        s.status_channel_key,
        s.cumulative_discount,
        s.seq_no,
        s.shipping_fee_bucket,
        s.row_seq,
        latest.unit_price,
        latest.quantity
    from valid s
    left join lateral (
            select c.unit_price, c.quantity
            from order_items c
            where c.order_id = s.id
            order by c.quantity desc, c.id desc
            limit 1
        ) latest on true
),
with_latest as (
    -- Latest matching payment per row (LATERAL ... LIMIT 1)
    select s.*, top1.payment_method
    from latest_match s
    join lateral (
            select c.method as payment_method
            from payments c
            where c.order_id = s.id
            order by c.amount desc, c.id desc
            limit 1
        ) top1 on true
)
-- Grouped metrics
select
    date_trunc('year', order_date) as year_start,
    size(collect_set(payment_method)) as num_payment_methods,
    size(collect_set(payment_method)) as num_payment_methods_2,
    count(*) filter (where payment_method = 'promptpay') as promptpay_rows
from with_latest
group by date_trunc('year', order_date)
order by promptpay_rows desc
