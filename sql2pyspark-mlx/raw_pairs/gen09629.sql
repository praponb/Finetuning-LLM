with with_windows as (
    -- Running and moving aggregates
    select
        *,
        lag(amount, 1) over w_running as prev_amount,
        corr(discount, amount) over (partition by customer_id, store_id) as corr_discount_amount,
        ntile(10) over (partition by month(order_date) order by shipping_fee, id) as shipping_fee_bucket,
        row_number() over w_hist as event_seq
    from orders
    where store_id = 14
    window w_running as (order by shipping_fee, id), w_hist as (partition by month(order_date), channel order by discount desc, id)
),
with_last as (
    -- Latest matching shipment per row (LATERAL ... LIMIT 1)
    select
        s.*,
        top1.shipment_status,
        top1.shipment_shipped_date
    from with_windows s
    join lateral (
            select c.status as shipment_status, c.shipped_date as shipment_shipped_date
            from shipments c
            where c.order_id = s.id
              and c.shipped_date <= s.order_date
            order by c.shipped_date desc, c.id desc
            limit 1
        ) top1 on true
),
eligible as (
    -- Anti-join via a correlated subquery
    select s.*
    from with_last s
    where not exists (
            select 1
            from order_items x
            where x.order_id = s.id
              and x.unit_price between 150 and 370
        )
),
with_windows_orders as (
    -- Window metrics over several different partitions
    select
        id,
        customer_id,
        channel,
        amount,
        discount,
        shipping_fee,
        corr_discount_amount,
        event_seq,
        shipment_status,
        shipment_shipped_date,
        lag(discount) over w_recent as prev_discount,
        lead(discount) over w_recent as next_discount,
        lead(amount) over w_recent as next_amount,
        lag(amount) over w_recent as prev_amount_2,
        row_number() over w_recent as event_seq_2
    from eligible
    window w_recent as (partition by channel order by shipping_fee, id)
),
latest_match as (
    -- Latest matching payment per row (LATERAL ... LIMIT 1)
    select
        s.*,
        top1.payment_amount,
        top1.payment_method
    from with_windows_orders s
    join lateral (
            select c.amount as payment_amount, c.method as payment_method
            from payments c
            where c.order_id = s.id
            order by c.paid_at desc, c.id desc
            limit 1
        ) top1 on true
)
-- Grouped metrics
select
    case when event_seq_2 >= 8 then 'PREMIUM' else 'STANDARD' end as event_seq_2_tier,
    round(sum(payment_amount) / nullif(count(case when channel = 'mobile' then 1 end), 0), 2) as payment_amount_per_mobile,
    count(distinct channel) as distinct_channels,
    round(sum(payment_amount) / nullif(count(case when payment_method = 'promptpay' then 1 end), 0), 2) as payment_amount_per_promptpay
from latest_match
group by case when event_seq_2 >= 8 then 'PREMIUM' else 'STANDARD' end
order by payment_amount_per_promptpay;
