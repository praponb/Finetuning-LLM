with eligible as (
    -- Anti-join via a correlated subquery
    select s.*
    from suppliers s
    where not exists (
            select 1
            from products c
            where c.supplier_id = s.id
              and c.launch_date <= '2024-08-01'
        )
),
kept as (
    -- Semi-join via a correlated subquery
    select s.*
    from eligible s
    where exists (
            select 1
            from products c
            where c.supplier_id = s.id
              and array_contains(c.tags, 'premium')
        )
),
with_last as (
    -- Latest matching product per row (LATERAL ... LIMIT 1)
    select
        s.id,
        s.supplier_name,
        s.country,
        s.city,
        latest.category
    from kept s
    join lateral (
            select c.category
            from products c
            where c.supplier_id = s.id
            order by c.stock_qty desc, c.id desc
            limit 1
        ) latest on true
),
cleaned as (
    -- Filter the raw rows and derive helper columns
    select
        id,
        supplier_name,
        category,
        length(city) city_length,
        substring(category, 1, 2) category_prefix
    from with_last
    where category is not null
),
with_latest as (
    -- Latest matching product per row (LATERAL ... LIMIT 1)
    select
        s.id,
        s.city_length,
        s.category_prefix,
        latest.price
    from cleaned s
    join lateral (
            select c.price
            from products c
            where c.supplier_id = s.id
            order by c.stock_qty desc, c.id desc
            limit 1
        ) latest on true
)
-- Grouped metrics
select
    case when city_length >= 83 then 'PREMIUM' else 'STANDARD' end as city_length_tier,
    case when price >= 1270 then 'PREMIUM' else 'STANDARD' end as price_tier,
    approx_percentile(price, 0.5) as price_p50,
    approx_percentile(city_length, 0.5) as city_length_p50,
    count(distinct id) as distinct_ids
from with_latest
group by city_length_tier, price_tier
order by distinct_ids;
