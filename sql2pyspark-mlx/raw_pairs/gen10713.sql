with valid as (
    -- Basic cleansing and defaults
    select
        id,
        city,
        region,
        opened_date,
        lower(city) as city_lower,
        case when city in ('London', 'Tokyo') then 'Tier 1' else 'Other' end as city_group
    from stores
    where opened_date > to_date('2023-07-17')
      and opened_date between '2024-07-06' and '2025-09-11'
),
scored as (
    -- Running and moving aggregates
    select *, first_value(region) ignore nulls over (partition by city order by opened_date, id rows between unbounded preceding and current row) as first_known_region
    from valid
    where region in ('North', 'South')
),
with_last as (
    -- Latest matching store_sale per row (LATERAL ... LIMIT 1)
    select
        s.id,
        s.city,
        s.region,
        s.opened_date,
        s.city_lower,
        s.city_group,
        s.first_known_region,
        top1.store_sale_revenue,
        top1.store_sale_sale_date
    from scored s
    left join lateral (
            select c.revenue as store_sale_revenue, c.sale_date as store_sale_sale_date
            from store_sales c
            where c.store_id = s.id
              and c.sale_date <= s.opened_date
            order by c.sale_date desc, c.id desc
            limit 1
        ) top1 on true
),
matched_keys as (
    -- Set intersection on keys
    select id, region
    from with_last
    intersect all
    select id, region
    from stores
    where store_name like 'Riv%'
),
reconciled as (
    -- Keep rows whose keys survive the set operation
    select s.*
    from with_last s
    inner join matched_keys k on s.id = k.id and s.region = k.region
)
select
    id,
    city,
    region,
    opened_date,
    store_sale_revenue,
    store_sale_sale_date
from reconciled
