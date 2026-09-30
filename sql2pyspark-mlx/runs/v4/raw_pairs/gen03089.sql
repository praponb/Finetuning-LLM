with filtered as (
    -- Basic cleansing and defaults
    select
        *,
        stock_qty - price as stock_qty_minus_price,
        coalesce(brand, 'OTHER') as brand_clean
    from products
    where cost > 230
),
eligible as (
    -- Semi-join via a correlated subquery
    select s.*
    from filtered s
    where exists (
            select 1
            from store_sales x
            where x.product_id = s.id
              and x.units > 20
        )
),
with_latest as (
    -- Latest matching order_item per row (LATERAL ... LIMIT 1)
    select
        s.*,
        x.unit_price,
        x.quantity
    from eligible s
    left join lateral (
            select c.unit_price, c.quantity
            from order_items c
            where c.product_id = s.id
            order by c.quantity desc, c.id desc
            limit 1
        ) x on true
),
deduped as (
    -- QUALIFY-style window filter
    select *, row_number() over (partition by brand order by price desc nulls last, id asc) as rn
    from with_latest
    qualify rn = 1
)
select
    s.id,
    s.product_name,
    s.category,
    s.brand,
    s.price,
    s.cost,
    s.stock_qty,
    s.tags,
    s.supplier_id,
    case when product_name rlike '^(Running Shoes|Smart Watch)$' then 'EXTENDED' else 'PRIORITY' end as product_name_zone,
    (select avg(t2.cost) from products t2 where t2.supplier_id = s.supplier_id) as avg_product_cost
from deduped s
order by id asc;
