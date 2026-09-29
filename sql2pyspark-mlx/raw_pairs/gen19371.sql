with cleaned as (
    select *, lower(method) as method_lower
    from payments
    where paid_at > date '2023-06-13'
),
kept_keys as (
    select status
    from cleaned
    except all
    select status
    from payments
    where status = 'REFUNDED'
),
verified as (
    select s.*
    from cleaned s
    inner join kept_keys k on s.status = k.status
),
enriched as (
    select e.*, o.shipping_fee
    from verified e
    left join orders o
        on e.order_id = o.id
),
enriched_payments as (
    select src.*, o.amount as order_amount
    from enriched src
    left join orders o
        on src.order_id = o.id
),
kept_keys_2 as (
    select status
    from enriched_payments
    intersect all
    select status
    from payments
    where status in ('REFUNDED', 'SUCCESS', 'FAILED')
),
kept_rows as (
    select s.*
    from enriched_payments s
    inner join kept_keys_2 k on s.status = k.status
)
select
    o.channel,
    s.status as payment_status,
    count(*) as num_rows,
    max(s.order_amount) as max_order_amount,
    grouping_id() as grouping_level
from kept_rows s
left join orders o on s.order_id = o.id
group by grouping sets ((o.channel, s.status), (o.channel))
order by max_order_amount desc;
