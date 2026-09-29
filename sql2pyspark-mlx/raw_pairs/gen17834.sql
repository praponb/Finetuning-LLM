with base as (
    -- Keep recent, valid rows
    select *, round(discount, 2) as discount_rounded
    from orders
    where order_date > date '2023-04-22'
),
eligible as (
    -- Anti-join via a correlated subquery
    select s.*
    from base s
    where not exists (
            select 1
            from order_items c
            where c.order_id = s.id
              and c.quantity between 6 and 8
        )
),
eligible_orders as (
    -- Semi-join via a correlated subquery
    select s.*
    from eligible s
    where exists (
            select 1
            from payments c
            where c.order_id = s.id
              and to_date(c.paid_at) >= '2024-10-02'
        )
),
cleaned as (
    -- Keep recent, valid rows
    select
        id,
        customer_id,
        store_id,
        order_date,
        channel,
        amount,
        shipping_fee,
        discount_rounded,
        amount + shipping_fee as amount_with_shipping,
        last_day(order_date) as order_date_month_end,
        coalesce(channel, 'OTHER') as resolved_channel
    from eligible_orders
    where customer_id is not null
      and channel is distinct from 'web'
)
select
    s.id,
    s.store_id,
    s.channel,
    s.amount,
    s.shipping_fee,
    s.discount_rounded,
    s.amount_with_shipping,
    s.order_date_month_end,
    s.resolved_channel,
    (select sum(o2.shipping_fee) from orders o2 where o2.store_id = s.store_id) as sum_order_shipping_fee
from cleaned s
order by amount desc nulls first, order_date_month_end;
