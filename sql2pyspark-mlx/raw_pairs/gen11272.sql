with base as (
    -- Basic cleansing and defaults
    select
        id,
        customer_id,
        store_id,
        status,
        channel,
        amount,
        floor(discount) discount_whole
    from orders
    where order_date > date '2023-09-19'
),
latest_match as (
    -- Latest matching order_item per row (LATERAL ... LIMIT 1)
    select
        s.*,
        top1.unit_price,
        top1.quantity
    from base s
    left join lateral (
            select c.unit_price, c.quantity
            from order_items c
            where c.order_id = s.id
            order by c.unit_price desc, c.id desc
            limit 1
        ) top1 on true
),
matched_keys as (
    -- Set difference on keys
    select store_id, id
    from latest_match
    except all
    select store_id, id
    from orders
    where store_id in (2, 11, 18)
),
reconciled as (
    -- Keep rows whose keys survive the set operation
    select s.*
    from latest_match s
    join matched_keys k on s.store_id = k.store_id and s.id = k.id
),
joined as (
    -- Attach customers attributes
    select
        a.*,
        b.segment,
        b.credit_limit,
        b.signup_date
    from reconciled a
    left join customers b
        on a.customer_id = b.id
)
select
    s.id,
    s.customer_id,
    s.store_id,
    s.status,
    s.channel,
    s.amount,
    s.discount_whole,
    s.segment,
    (select min(o2.shipping_fee) from orders o2 where o2.customer_id = s.customer_id) as min_order_shipping_fee
from joined s
