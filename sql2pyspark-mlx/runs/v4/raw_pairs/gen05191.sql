with joined as (
    -- Attach suppliers attributes
    select
        a.*,
        d.country,
        d.city,
        d.rating
    from products a
    left join suppliers d
        on a.supplier_id = d.id
),
cleaned as (
    -- Keep recent, valid rows
    select
        id,
        category,
        brand,
        price,
        stock_qty,
        tags,
        supplier_id,
        size(tags) as tags_count,
        coalesce(city, 'OTHER') as city_filled
    from joined
    where product_name like 'wir%'
),
eligible as (
    -- Semi-join via a correlated subquery
    select s.*
    from cleaned s
    where exists (
            select 1
            from inventory c
            where c.product_id = s.id
              and c.quantity >= 131
        )
),
with_last as (
    -- Latest matching inventory per row (LATERAL ... LIMIT 1)
    select
        s.*,
        top1.quantity,
        top1.last_restock
    from eligible s
    left join lateral (
            select c.quantity, c.last_restock
            from inventory c
            where c.product_id = s.id
            order by c.quantity desc, c.id desc
            limit 1
        ) top1 on true
),
joined_products as (
    -- Attach suppliers attributes
    select
        s.*,
        b.country,
        b.supplier_name
    from with_last s
    left join suppliers b
        on s.supplier_id = b.id
),
cleaned_products as (
    -- Filter the raw rows and derive helper columns
    select *, initcap(trim(country)) as country_title
    from joined_products
    where supplier_id is not null
)
-- Native SQL PIVOT
select *
from (
    select
        brand,
        category,
        stock_qty
    from cleaned_products
) src
pivot (
    max(stock_qty)
    for category in ('Fashion', 'Home')
);
