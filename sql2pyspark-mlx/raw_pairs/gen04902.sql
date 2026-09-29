with eligible as (
    -- Semi-join via a correlated subquery
    select s.*
    from stores s
    where exists (
            select 1
            from store_sales c
            where c.store_id = s.id
              and year(c.sale_date) = 2023
        )
),
kept as (
    -- Anti-join via a correlated subquery
    select s.*
    from eligible s
    where not exists (
            select 1
            from store_sales x
            where x.store_id = s.id
              and x.sale_date <= s.opened_date
        )
),
latest as (
    -- Keep one row per key (latest first)
    select *, row_number() over (partition by region, city order by sqft, id) as rn
    from kept
    qualify rn = 1
),
base as (
    -- Filter the raw rows and derive helper columns
    select *, case when region = 'North' then 1 else 0 end as is_north
    from latest
    where opened_date >= '2025-04-01'
      and opened_date > to_date('2023-05-24')
),
eligible_stores as (
    -- Anti-join via a correlated subquery
    select s.*
    from base s
    where not exists (
            select 1
            from store_sales ch
            where ch.store_id = s.id
              and ch.sale_date < s.opened_date
        )
),
wide as (
    -- Native SQL PIVOT
    select *
    from (
        select
            city,
            region,
            sqft
        from eligible_stores
    ) src
    pivot (
        max(sqft)
        for region in ('West', 'South')
    )
)
select city, West
from wide
