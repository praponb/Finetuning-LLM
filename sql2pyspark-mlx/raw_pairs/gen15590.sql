with base as (
    -- Filter the raw rows and derive helper columns
    select
        *,
        trim(status) clean_status,
        coalesce(channel, 'DIRECT') channel_filled
    from orders
    where order_date >= to_date('2023-07-03')
),
kept as (
    -- Semi-join via a correlated subquery
    select s.*
    from base s
    where exists (
            select 1
            from shipments ch
            where ch.order_id = s.id
              and ch.delivered_date between '2023-06-01' and '2025-02-01'
        )
),
matched as (
    -- Non-equi join on a validity window
    select
        e.*,
        sub.id as subscription_id,
        sub.plan,
        sub.monthly_fee
    from kept e
    left join subscriptions sub
        on e.customer_id = sub.customer_id
       and e.order_date >= sub.start_date
       and e.order_date < sub.end_date + interval '7' day
),
kept_keys as (
    -- Set difference on keys
    select id, status
    from matched
    except
    select id, status
    from orders
    where shipping_fee < 39
),
reconciled as (
    -- Keep rows whose keys survive the set operation
    select s.*
    from matched s
    join kept_keys k on s.id = k.id and s.status = k.status
),
remaining_keys as (
    -- Set intersection on keys
    select status
    from reconciled
    intersect all
    select status
    from orders
    where order_date > '2025-02-01'
),
reconciled_orders as (
    -- Keep rows whose keys survive the set operation
    select s.*
    from reconciled s
    join remaining_keys k on s.status = k.status
),
not_flagged as (
    -- Semi-join via a correlated subquery
    select s.*
    from reconciled_orders s
    where exists (
            select 1
            from payments x
            where x.order_id = s.id
              and x.paid_at >= s.order_date
        )
),
remaining_keys_2 as (
    -- Set difference on keys
    select id
    from not_flagged
    except
    select id
    from orders
    where customer_id = 12
),
verified as (
    -- Keep rows whose keys survive the set operation
    select s.*
    from not_flagged s
    join remaining_keys_2 k on s.id = k.id
)
select
    s.id,
    s.customer_id,
    s.order_date,
    s.status,
    s.channel,
    s.amount,
    s.shipping_fee,
    s.channel_filled,
    s.subscription_id,
    (select max(o2.shipping_fee) from orders o2 where o2.customer_id = s.customer_id) as max_order_shipping_fee
from verified s
order by id asc;
