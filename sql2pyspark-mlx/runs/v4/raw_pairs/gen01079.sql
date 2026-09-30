with base as (
    -- Keep recent, valid rows
    select
        *,
        last_day(launch_date) launch_date_month_end,
        coalesce(brand, 'OTHER') resolved_brand
    from products
    where size(tags) > 1
),
with_latest as (
    -- Latest matching inventory per row (LATERAL ... LIMIT 1)
    select s.*, top1.inventory_quantity
    from base s
    join lateral (
            select c.quantity as inventory_quantity
            from inventory c
            where c.product_id = s.id
            order by c.quantity desc, c.id desc
            limit 1
        ) top1 on true
),
with_dims as (
    -- Attach suppliers attributes
    select
        src.*,
        d.supplier_name,
        d.country,
        d.rating
    from with_latest src
    left join suppliers d
        on src.supplier_id = d.id
)
select
    s.id,
    s.product_name,
    s.brand,
    s.price,
    s.cost,
    s.stock_qty,
    s.launch_date,
    s.tags,
    s.supplier_id,
    (select avg(t2.stock_qty) from products t2 where t2.supplier_id = s.supplier_id) as avg_product_stock_qty
from with_dims s
