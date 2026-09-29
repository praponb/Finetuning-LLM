with joined as (
    -- Attach suppliers attributes
    select
        a.*,
        p.rating,
        p.city
    from products a
    left join suppliers p
        on a.supplier_id = p.id
),
with_last as (
    -- Latest matching order_item per row (LATERAL ... LIMIT 1)
    select s.*, latest.unit_price
    from joined s
    left join lateral (
            select c.unit_price
            from order_items c
            where c.product_id = s.id
            order by c.quantity desc, c.id desc
            limit 1
        ) latest on true
),
ranked as (
    -- Keep one row per key (latest first)
    select *
    from (
        select *, row_number() over (partition by supplier_id, city order by rating desc nulls first, id asc) as rn
        from with_last
    ) ranked
    where rn = 1
),
wide as (
    -- Native SQL PIVOT
    select *
    from (
        select
            brand,
            category,
            cost
        from ranked
    ) src
    pivot (
        max(cost) as total,
        count(1) as n
        for category in ('Fashion', 'Grocery', 'Home')
    )
)
select
    brand,
    Fashion_n,
    Grocery_total,
    Grocery_n,
    Home_total
from wide
order by Grocery_total desc nulls first, Grocery_n desc
