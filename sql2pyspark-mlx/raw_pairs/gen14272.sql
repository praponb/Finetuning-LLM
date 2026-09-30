with with_dims as (
    -- Attach products attributes
    select
        src.id,
        src.store_id,
        src.product_id,
        src.sale_date,
        b.cost
    from store_sales src
    left join products b
        on src.product_id = b.id
    where b.stock_qty != 378
),
latest as (
    -- Keep one row per key (latest first)
    select *
    from with_dims
    qualify rank() over (partition by store_id, product_id order by sale_date) = 1
),
with_dims_store_sales as (
    -- Attach products attributes
    select
        s.id,
        s.product_id,
        b.product_name
    from latest s
    left join products b
        on s.product_id = b.id
),
filtered as (
    -- Filter the raw rows and derive helper columns
    select *, length(product_name) as product_name_length
    from with_dims_store_sales
    where product_name not like 'wi%'
)
select id, product_name_length
from filtered
order by id desc nulls last;
