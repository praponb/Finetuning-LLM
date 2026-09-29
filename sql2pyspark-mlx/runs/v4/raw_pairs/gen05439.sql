with with_window as (
    -- Non-equi join on a validity window
    select
        src.*,
        sub.id as subscription_id,
        sub.plan,
        sub.monthly_fee
    from orders src
    left join subscriptions sub
        on src.customer_id = sub.customer_id
       and src.order_date >= sub.start_date
       and src.order_date < sub.end_date + interval 30 days
),
not_flagged as (
    -- Anti-join via a correlated subquery
    select s.*
    from with_window s
    where not exists (
            select 1
            from order_items c
            where c.order_id = s.id
              and c.product_id is not null
        )
),
with_last as (
    -- Latest matching payment per row (LATERAL ... LIMIT 1)
    select s.*, top1.payment_amount
    from not_flagged s
    left join lateral (
            select c.amount as payment_amount
            from payments c
            where c.order_id = s.id
              and c.paid_at <= s.order_date
            order by c.paid_at desc, c.id desc
            limit 1
        ) top1 on true
),
joined as (
    -- Attach stores attributes
    select s.*, d.store_name
    from with_last s
    left join stores d
        on s.store_id = d.id
),
matched as (
    -- Non-equi join on a validity window
    select
        s.*,
        pr.id as promo_id,
        pr.promo_code,
        pr.discount_pct
    from joined s
    inner join promotions pr
        on coalesce(s.channel, 'web') = pr.channel
       and s.order_date >= pr.starts_on
       and s.order_date <= date_add(pr.ends_on, 10)
),
with_last_orders as (
    -- Latest matching shipment per row (LATERAL ... LIMIT 1)
    select
        s.*,
        x.shipment_shipped_date,
        x.shipment_carrier
    from matched s
    left join lateral (
            select c.shipped_date as shipment_shipped_date, c.carrier as shipment_carrier
            from shipments c
            where c.order_id = s.id
            order by c.shipped_date desc, c.id desc
            limit 1
        ) x on true
),
deduped as (
    -- Keep one row per key (latest first)
    select *, rank() over (partition by status, plan order by order_date desc) as dedup_rank
    from with_last_orders
    qualify dedup_rank = 1
)
-- Multi-level aggregation
select
    date_trunc('quarter', shipment_shipped_date) as quarter_start,
    promo_id,
    date_trunc('month', shipment_shipped_date) as activity_month,
    count(distinct channel) as distinct_channels,
    count(distinct plan) as distinct_plans,
    percentile_approx(monthly_fee, 0.5) as monthly_fee_p50,
    approx_percentile(discount, array(0.25, 0.5, 0.75)) as discount_quartiles,
    count(*) as row_count,
    round(sum(shipping_fee) / nullif(count(case when plan = 'standard' then 1 end), 0), 2) as shipping_fee_per_standard,
    dense_rank() over (partition by date_trunc('quarter', shipment_shipped_date) order by round(sum(shipping_fee) / nullif(count(case when plan = 'standard' then 1 end), 0), 2) desc) as shipping_fee_per_standard_dense_rank
from deduped
group by 1, 2, 3
order by shipping_fee_per_standard desc nulls last;
