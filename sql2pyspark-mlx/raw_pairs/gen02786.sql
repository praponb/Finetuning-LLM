with valid as (
    -- Filter the raw rows and derive helper columns
    select *, substring(status, 1, 2) as status_prefix
    from orders
    where amount >= 500
      and order_date >= to_date('2023-04-02')
),
not_flagged as (
    -- Anti-join via a correlated subquery
    select s.*
    from valid s
    where not exists (
            select 1
            from order_items ch
            where ch.order_id = s.id
              and ch.product_id = 26
        )
),
not_flagged_orders as (
    -- Semi-join via a correlated subquery
    select s.*
    from not_flagged s
    where exists (
            select 1
            from shipments c
            where c.order_id = s.id
              and c.delivered_date <= s.order_date
        )
),
enriched as (
    -- Ranking and gap features
    select
        *,
        corr(discount, amount) over (partition by store_id) as corr_discount_amount,
        corr(amount, shipping_fee) over (partition by status) as corr_amount_shipping_fee
    from not_flagged_orders
),
with_latest as (
    -- Latest matching payment per row (LATERAL ... LIMIT 1)
    select s.*, latest.payment_amount
    from enriched s
    join lateral (
            select c.amount as payment_amount
            from payments c
            where c.order_id = s.id
              and c.paid_at <= s.order_date
            order by c.paid_at desc, c.id desc
            limit 1
        ) latest on true
),
enriched_orders as (
    -- Attach customers attributes
    select
        e.id,
        e.customer_id,
        e.store_id,
        e.amount,
        e.shipping_fee,
        e.payment_amount,
        d.signup_date
    from with_latest e
    left join customers d
        on e.customer_id = d.id
),
latest as (
    -- Keep one row per key (latest first)
    select *
    from enriched_orders
    qualify row_number() over (partition by store_id, customer_id order by payment_amount nulls first, id) = 1
)
-- Aggregate with conditional metrics
select
    case when shipping_fee >= 33 then 'PREMIUM' else 'STANDARD' end as shipping_fee_tier,
    store_id,
    approx_percentile(shipping_fee, 0.9) as shipping_fee_p90,
    count(distinct store_id) as distinct_store_ids,
    count(*) as cnt
from latest
group by 1, 2
order by shipping_fee_p90 desc nulls last;
