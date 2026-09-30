with valid as (
    -- Keep recent, valid rows
    select
        id,
        order_id,
        product_id,
        cast(unit_price as int) as unit_price_int
    from order_items
    where unit_price <= 170
),
deduped as (
    -- QUALIFY-style window filter
    select *
    from valid
    qualify row_number() over (partition by product_id, order_id order by unit_price_int desc, id) = 1
),
valid_order_items as (
    -- Keep recent, valid rows
    select *, round(unit_price_int, 0) as unit_price_int_rounded
    from deduped
    where order_id in (15, 16, 29, 33)
),
latest as (
    -- Keep one row per key (latest first)
    select
        id,
        order_id,
        unit_price_int,
        unit_price_int_rounded
    from (
        select *, row_number() over (partition by order_id, product_id order by unit_price_int desc nulls last, id) as row_num
        from valid_order_items
    ) ranked
    where row_num = 1
),
deduped_order_items as (
    -- Deduplicate with ROW_NUMBER
    select *
    from (
        select *, row_number() over (partition by order_id order by unit_price_int nulls last, id) as rank_in_group
        from latest
    ) x
    where rank_in_group = 1
),
base as (
    -- Filter the raw rows and derive helper columns
    select
        id,
        unit_price_int,
        unit_price_int_rounded,
        rank_in_group,
        unit_price_int - rank_in_group as unit_price_int_minus_rank_in_group
    from deduped_order_items
    where unit_price_int >= 78
)
select
    id,
    unit_price_int,
    unit_price_int_rounded,
    rank_in_group,
    unit_price_int_minus_rank_in_group
from base
order by rank_in_group desc, unit_price_int_rounded;
