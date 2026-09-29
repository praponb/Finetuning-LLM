with filtered as (
    -- Keep recent, valid rows
    select
        id,
        supplier_name,
        country,
        city,
        rating,
        coalesce(rating, 0) rating_filled
    from suppliers
    where rating > 3.0
),
base as (
    -- Keep recent, valid rows
    select
        id,
        supplier_name,
        city,
        rating,
        rating_filled,
        concat_ws('-', city, country) city_country_key,
        coalesce(rating_filled, 0) rating_filled_filled
    from filtered
    where city is not null
),
latest as (
    -- Keep one row per key (latest first)
    select *, row_number() over (partition by city order by rating desc, id) as rn
    from base
    qualify rn = 1
),
base_suppliers as (
    -- Filter the raw rows and derive helper columns
    select
        id,
        city,
        rating,
        rating_filled,
        rn,
        length(supplier_name) supplier_name_length,
        case when city in ('Bangkok', 'London') then 'Group A' else 'Other' end city_group
    from latest
    where city is not null
),
eligible as (
    -- Anti-join via a correlated subquery
    select s.*
    from base_suppliers s
    where not exists (
            select 1
            from products c
            where c.supplier_id = s.id
              and length(c.product_name) > 13
        )
)
-- Multi-level aggregation
select
    case when rating >= 3.4 then 'PREMIUM' else 'STANDARD' end as rating_tier,
    round(sum(rating) / nullif(count(case when city = 'Tokyo' then 1 end), 0), 2) as rating_per_tokyo,
    percentile_approx(rating, 0.95) as rating_p95
from eligible
group by case when rating >= 3.4 then 'PREMIUM' else 'STANDARD' end
having percentile_approx(rating, 0.95) > 0
