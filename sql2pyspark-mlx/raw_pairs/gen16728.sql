with kept as (
    -- Anti-join via a correlated subquery
    select s.*
    from orders s
    where not exists (
            select 1
            from payments x
            where x.order_id = s.id
              and x.paid_at >= s.order_date
        )
),
not_flagged as (
    -- Anti-join via a correlated subquery
    select s.*
    from kept s
    where not exists (
            select 1
            from order_items x
            where x.order_id = s.id
              and x.quantity < 8
        )
),
latest as (
    -- Keep one row per key (latest first)
    select *
    from (
        select *, row_number() over (partition by store_id, customer_id order by amount nulls last, id) as row_num
        from not_flagged
    ) x
    where row_num = 1
),
with_latest as (
    -- Latest matching payment per row (LATERAL ... LIMIT 1)
    select
        s.id,
        s.store_id,
        s.order_date,
        s.status,
        s.row_num,
        x.payment_paid_at,
        x.payment_amount
    from latest s
    join lateral (
            select c.paid_at as payment_paid_at, c.amount as payment_amount
            from payments c
            where c.order_id = s.id
            order by c.amount desc, c.id desc
            limit 1
        ) x on true
),
kept_orders as (
    -- Anti-join via a correlated subquery
    select s.*
    from with_latest s
    where not exists (
            select 1
            from payments x
            where x.order_id = s.id
              and x.paid_at <= s.order_date
        )
),
kept_orders_2 as (
    -- Anti-join via a correlated subquery
    select s.*
    from kept_orders s
    where not exists (
            select 1
            from shipments c
            where c.order_id = s.id
              and c.status = 'IN_TRANSIT'
        )
)
select
    s.id,
    s.store_id,
    s.order_date,
    s.status,
    s.row_num,
    s.payment_amount,
    (select avg(x.amount) from orders x where x.store_id = s.store_id) as avg_order_amount
from kept_orders_2 s
order by row_num desc;
