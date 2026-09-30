with valid as (
    -- Basic cleansing and defaults
    select
        id,
        store_name,
        city,
        region,
        coalesce(region, 'unassigned') region_filled,
        substring(city, 1, 4) city_prefix
    from stores
    where opened_date >= date '2023-09-20'
      and region is not null
),
with_latest as (
    -- Latest matching inventory per row (LATERAL ... LIMIT 1)
    select
        s.id,
        s.city,
        s.region,
        s.region_filled,
        s.city_prefix,
        top1.last_restock,
        top1.quantity
    from valid s
    join lateral (
            select c.last_restock, c.quantity
            from inventory c
            where c.store_id = s.id
            order by c.last_restock desc, c.id desc
            limit 1
        ) top1 on true
),
kept as (
    -- Anti-join via a correlated subquery
    select s.*
    from with_latest s
    where not exists (
            select 1
            from inventory ch
            where ch.store_id = s.id
              and ch.last_restock < s.last_restock
        )
),
deduped as (
    -- Keep one row per key (latest first)
    select *, row_number() over (partition by city order by quantity, id) as dedup_rank
    from kept
    qualify dedup_rank = 1
),
stats as (
    -- Aggregate with conditional metrics
    select
        date_trunc('year', last_restock) as year_start,
        case when quantity >= 189 then 'PREMIUM' else 'STANDARD' end as quantity_tier,
        percentile_approx(quantity, array(0.25, 0.5, 0.75)) as quantity_quartiles,
        round(sum(quantity) / nullif(count(case when city = 'Tokyo' then 1 end), 0), 2) as quantity_per_tokyo,
        count(*) as cnt,
        count(distinct city) as distinct_cities,
        sum(case when region = 'West' then quantity else 0 end) as west_quantity,
        round(sum(quantity) / nullif(count(case when city = 'Bangkok' then 1 end), 0), 2) as quantity_per_bangkok,
        grouping(date_trunc('year', last_restock)) as grouping_level
    from deduped
    group by rollup (date_trunc('year', last_restock), case when quantity >= 189 then 'PREMIUM' else 'STANDARD' end)
)
select
    year_start,
    quantity_tier,
    quantity_quartiles,
    quantity_per_tokyo,
    cnt,
    west_quantity,
    quantity_per_bangkok
from stats
order by quantity_tier desc;
