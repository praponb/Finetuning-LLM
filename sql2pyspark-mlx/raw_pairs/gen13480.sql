with base as (
    -- Basic cleansing and defaults
    select
        *,
        case when country in ('SG', 'US') then 'Group A' else 'Other' end as country_group,
        case when rating >= 4.5 then 'High' when rating >= 2.0 then 'Medium' else 'Low' end as rating_tier
    from suppliers
    where city is not null
),
eligible as (
    -- Anti-join via a correlated subquery
    select s.*
    from base s
    where not exists (
            select 1
            from products c
            where c.supplier_id = s.id
              and c.cost > 440
        )
),
not_flagged as (
    -- Semi-join via a correlated subquery
    select s.*
    from eligible s
    where exists (
            select 1
            from products c
            where c.supplier_id = s.id
              and month(c.launch_date) in (4, 9, 12)
        )
),
with_latest as (
    -- Latest matching product per row (LATERAL ... LIMIT 1)
    select s.*, x.product_category
    from not_flagged s
    left join lateral (
            select c.category as product_category
            from products c
            where c.supplier_id = s.id
            order by c.launch_date desc, c.id desc
            limit 1
        ) x on true
)
select
    id,
    supplier_name,
    country,
    rating
from with_latest;
