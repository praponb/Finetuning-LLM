with base as (
    -- Filter the raw rows and derive helper columns
    select
        *,
        size(tags) as tags_count,
        coalesce(brand, 'NONE') as brand_clean
    from products
    where launch_date > date '2023-03-30'
      and year(launch_date) >= 2025
)
-- QUALIFY-style window filter
select
    id,
    product_name,
    brand,
    price,
    cost,
    tags,
    supplier_id
from (
    select *, rank() over (partition by brand order by stock_qty desc) as rn
    from base
) x
where rn = 1;
