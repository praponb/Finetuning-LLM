with filtered as (
    -- Keep recent, valid rows
    select
        id,
        store_name,
        city,
        region,
        sqft,
        datediff(current_date(), opened_date) / 7 as weeks_since_opened_date
    from stores
    where opened_date >= to_date('2023-05-24')
      and sqft between 11231 and 14925
),
filtered_stores as (
    -- Basic cleansing and defaults
    select
        *,
        concat_ws('-', region, city) as region_city_key,
        coalesce(region, 'DIRECT') as region_clean
    from filtered
    where store_name like 'O%'
),
eligible as (
    -- Semi-join via a correlated subquery
    select s.*
    from filtered_stores s
    where exists (
            select 1
            from inventory c
            where c.store_id = s.id
              and c.product_id in (1, 7, 10)
        )
),
not_flagged as (
    -- Anti-join via a correlated subquery
    select s.*
    from eligible s
    where not exists (
            select 1
            from inventory c
            where c.store_id = s.id
              and c.product_id in (3, 4, 6)
        )
),
deduped as (
    -- Keep one row per key (latest first)
    select *
    from (
        select *, row_number() over (partition by city, region order by weeks_since_opened_date desc, id) as dedup_rank
        from not_flagged
    ) t
    where dedup_rank = 1
)
select
    id,
    store_name,
    city,
    region,
    weeks_since_opened_date,
    region_clean,
    dedup_rank,
    case when region rlike '^(East|Central)$' then 'PRIORITY' else 'GROUP_B' end as region_zone
from deduped;
