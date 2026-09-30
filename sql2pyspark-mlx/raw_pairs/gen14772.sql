with exploded as (
    -- Unnest the array into one row per element
    select
        id,
        customer_id,
        coupon_codes,
        inline(items)
    from carts
),
metrics as (
    -- Running and moving aggregates
    select
        id,
        customer_id,
        qty,
        max(product_id) over (partition by customer_id) as customer_id_max_product_id
    from exploded
),
with_dims as (
    -- Attach customers attributes
    select
        e.id,
        e.customer_id,
        e.customer_id_max_product_id,
        c.age,
        c.name,
        c.credit_limit
    from metrics e
    join customers c
        on e.customer_id = c.id
),
enriched as (
    -- Attach customers attributes
    select
        a.id,
        a.customer_id,
        a.customer_id_max_product_id,
        a.credit_limit,
        c.signup_date,
        c.email
    from with_dims a
    left join customers c
        on a.customer_id = c.id
    where c.signup_date < '2024-07-01'
)
select
    id,
    customer_id,
    customer_id_max_product_id,
    credit_limit
from enriched
order by customer_id_max_product_id desc;
