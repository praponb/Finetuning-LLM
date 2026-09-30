with exploded as (
    -- Unnest the array into one row per element
    select
        id,
        created_at,
        coupon_codes,
        li_item.product_id,
        li_item.qty,
        li_item.price,
        li_item.promo
    from carts
    lateral view explode(items) x as li_item
),
exploded_carts as (
    -- Unnest the array into one row per element
    select
        id,
        created_at,
        product_id,
        qty,
        promo,
        code
    from exploded
    lateral view outer explode(coupon_codes) e as code
),
remaining_keys as (
    -- Set difference on keys
    select id
    from exploded_carts
    except distinct
    select id
    from carts
    where created_at < '2024-11-01'
),
kept_rows as (
    -- Keep rows whose keys survive the set operation
    select s.*
    from exploded_carts s
    inner join remaining_keys k on s.id = k.id
),
valid as (
    -- Filter the raw rows and derive helper columns
    select
        id,
        created_at,
        qty,
        promo,
        code,
        datediff(current_date(), created_at) / 30 as months_since_created_at,
        qty + product_id as qty_plus_product_id
    from kept_rows
    where product_id between 6 and 7
)
-- Window metrics over several different partitions
select
    id,
    created_at,
    qty,
    promo,
    code,
    count(qty_plus_product_id) over (partition by promo) as promo_count_qty_plus_product_id,
    cume_dist() over (partition by promo order by months_since_created_at desc nulls last) as months_since_created_at_cume_dist
from valid
