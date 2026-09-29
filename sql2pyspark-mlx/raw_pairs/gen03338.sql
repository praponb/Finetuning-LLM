with filtered as (
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
        date_add(order_date, 79) as order_date_plus_79d,
        least(shipping_fee, discount) as least_shipping_fee_discount,
        coalesce(channel, 'DIRECT') as channel_filled
    from orders
    where order_date >= date '2023-06-13'
      and channel is not null
),
kept_keys as (
    -- Set intersection on keys
    select store_id, id
    from filtered
    intersect all
    select store_id, id
    from orders
    where year(order_date) = 2023
),
verified as (
    -- Keep rows whose keys survive the set operation
    select s.*
    from filtered s
    inner join kept_keys k on s.store_id = k.store_id and s.id = k.id
),
remaining_keys as (
    -- Set intersection on keys
    select store_id, status
    from verified
    intersect all
    select store_id, status
    from orders
    where customer_id is not null
),
reconciled as (
    -- Keep rows whose keys survive the set operation
    select s.*
    from verified s
    inner join remaining_keys k on s.store_id = k.store_id and s.status = k.status
),
not_flagged as (
    -- Anti-join via a correlated subquery
    select s.*
    from reconciled s
    where not exists (
            select 1
            from payments ch
            where ch.order_id = s.id
              and ch.paid_at < s.order_date_plus_79d
        )
),
with_windows as (
    -- Running and moving aggregates
    select
        *,
        dense_rank() over (partition by status order by amount desc nulls first) as amount_dense_rank,
        cume_dist() over (partition by status order by amount nulls last) as amount_cume_dist,
        round(amount / sum(amount) over (partition by channel), 4) as amount_share
    from not_flagged
),
attributed as (
    -- Non-equi join on a validity window
    select
        src.*,
        pr.id as promo_id,
        pr.promo_code,
        pr.discount_pct
    from with_windows src
    inner join promotions pr
        on coalesce(src.channel, 'store') = pr.channel
       and src.order_date_plus_79d between pr.starts_on and pr.ends_on
),
eligible as (
    -- Anti-join via a correlated subquery
    select s.*
    from attributed s
    where not exists (
            select 1
            from order_items ch
            where ch.order_id = s.id
              and ch.unit_price <= 240
        )
)
-- Join a dimension and aggregate
select
    p.segment,
    p.city,
    count(*) as rows_total,
    sum(src.least_shipping_fee_discount) as total_least_shipping_fee_discount
from eligible src
inner join customers p on src.customer_id = p.id
group by p.segment, p.city
order by city desc nulls last
