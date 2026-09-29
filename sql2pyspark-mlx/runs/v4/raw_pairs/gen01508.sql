with filtered as (
    -- Basic cleansing and defaults
    select *, ceil(quantity) as quantity_whole
    from inventory
    where last_restock >= date '2023-07-22'
),
base as (
    -- Filter the raw rows and derive helper columns
    select
        id,
        store_id,
        product_id,
        quantity,
        last_restock,
        quantity_whole,
        date_add(last_restock, 58) as last_restock_plus_58d
    from filtered
    where last_restock > to_date('2023-04-29')
),
latest as (
    -- QUALIFY-style window filter
    select *
    from (
        select *, rank() over (partition by product_id order by last_restock_plus_58d desc) as rank_in_group
        from base
    ) t
    where rank_in_group = 1
),
enriched as (
    -- Attach products attributes
    select s.*, b.stock_qty
    from latest s
    left join products b
        on s.product_id = b.id
),
report as (
    select
        id,
        store_id,
        product_id,
        quantity,
        last_restock,
        quantity_whole,
        rank_in_group
    from enriched
    order by store_id desc nulls last
)
select
    id,
    product_id,
    quantity,
    last_restock
from report
order by last_restock desc, product_id desc;
