with valid as (
    -- Keep recent, valid rows
    select
        *,
        lower(supplier_name) supplier_name_lower,
        coalesce(city, 'UNKNOWN') city_clean
    from suppliers
    where city <> 'Bangkok'
)
-- Latest matching product per row (LATERAL ... LIMIT 1)
select
    s.id,
    s.city_clean,
    latest.product_launch_date,
    latest.product_stock_qty
from valid s
join lateral (
        select c.launch_date as product_launch_date, c.stock_qty as product_stock_qty
        from products c
        where c.supplier_id = s.id
        order by c.price desc, c.id desc
        limit 1
    ) latest on true;
