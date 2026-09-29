with base as (
    -- Basic cleansing and defaults
    select
        *,
        lower(supplier_name) as supplier_name_lower,
        coalesce(city, 'N/A') as city_clean
    from suppliers
    where city is not null
),
with_latest as (
    -- Latest matching product per row (LATERAL ... LIMIT 1)
    select s.*, x.product_cost
    from base s
    join lateral (
            select c.cost as product_cost
            from products c
            where c.supplier_id = s.id
            order by c.cost desc, c.id desc
            limit 1
        ) x on true
),
ranked as (
    -- Deduplicate with ROW_NUMBER
    select *
    from (
        select *, row_number() over (partition by city order by product_cost desc nulls first, id) as rn
        from with_latest
    ) t
    where rn = 1
),
not_flagged as (
    -- Semi-join via a correlated subquery
    select s.*
    from ranked s
    where exists (
            select 1
            from products ch
            where ch.supplier_id = s.id
              and ch.category != 'Home'
        )
)
-- Grouped metrics
select
    city,
    count(distinct country) as distinct_countries,
    sum(rating) as total_rating,
    count(case when country = 'US' then 1 end) as us_count,
    round(sum(product_cost) / nullif(count(case when country = 'US' then 1 end), 0), 2) as product_cost_per_us,
    count(*) filter (where city = 'London') as london_rows,
    max(product_cost) as max_product_cost
from not_flagged
group by city
order by max_product_cost desc nulls first
