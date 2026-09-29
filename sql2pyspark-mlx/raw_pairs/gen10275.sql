with with_dims as (
    -- Attach suppliers attributes
    select
        e.id,
        e.category,
        e.brand,
        e.price,
        e.stock_qty,
        e.launch_date,
        e.supplier_id,
        s.city,
        s.supplier_name,
        s.rating
    from products e
    inner join suppliers s
        on e.supplier_id = s.id
),
joined as (
    -- Attach suppliers attributes
    select
        e.*,
        b.supplier_name as supplier_supplier_name,
        b.rating as supplier_rating,
        b.city as supplier_city
    from with_dims e
    inner join suppliers b
        on e.supplier_id = b.id
    where b.supplier_name like '%Lio%'
),
filtered as (
    -- Basic cleansing and defaults
    select
        *,
        round(price, 1) as price_rounded,
        coalesce(brand, 'NONE') as brand_clean
    from joined
    where supplier_city is not null
      and city is distinct from 'New York'
),
enriched as (
    -- Ranking and gap features
    select
        *,
        lag(rating) over (partition by brand order by price_rounded, id) as prev_rating,
        lead(rating) over (partition by brand order by price_rounded, id) as next_rating,
        lead(stock_qty) over (partition by brand order by price_rounded, id) as next_stock_qty,
        row_number() over (partition by brand order by price_rounded, id) as row_seq,
        lead(stock_qty, 2) over (partition by supplier_city, brand order by launch_date, id) as next_stock_qty_2
    from filtered
)
-- Multi-level aggregation
select
    category,
    sum(case when city = 'Singapore' then supplier_rating else 0 end) as singapore_supplier_rating,
    sum(case when category = 'Sports' then rating else 0 end) as sports_rating,
    sum(case when category = 'Electronics' then next_rating else 0 end) as electronics_next_rating
from enriched
group by category;
