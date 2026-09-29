with valid as (
    -- Filter the raw rows and derive helper columns
    select *, length(region) region_length
    from stores
    where sqft = 12476
),
with_latest as (
    -- Latest matching order per row (LATERAL ... LIMIT 1)
    select
        s.id,
        s.store_name,
        s.city,
        s.region,
        s.opened_date,
        s.region_length,
        latest.order_date
    from valid s
    join lateral (
            select c.order_date
            from orders c
            where c.store_id = s.id
            order by c.amount desc, c.id desc
            limit 1
        ) latest on true
),
kept_keys as (
    -- Set difference on keys
    select region, city
    from with_latest
    except
    select region, city
    from stores
    where sqft >= 15201
),
verified as (
    -- Keep rows whose keys survive the set operation
    select s.*
    from with_latest s
    join kept_keys k on s.region = k.region and s.city = k.city
),
kept_keys_2 as (
    -- Set difference on keys
    select region, city
    from verified
    except distinct
    select region, city
    from stores
    where sqft < 15684
),
verified_stores as (
    -- Keep rows whose keys survive the set operation
    select s.*
    from verified s
    join kept_keys_2 k on s.region = k.region and s.city = k.city
),
kept as (
    -- Anti-join via a correlated subquery
    select s.*
    from verified_stores s
    where not exists (
            select 1
            from inventory ch
            where ch.store_id = s.id
              and ch.last_restock <= s.order_date
        )
)
-- Grouped metrics
select
    date_trunc('month', order_date) as activity_month,
    region,
    sum(region_length) as total_region_length,
    sort_array(collect_set(region)) as region_list,
    sum(case when city = 'Tokyo' then region_length else 0 end) as tokyo_region_length,
    grouping_id() as gid
from kept
group by rollup (date_trunc('month', order_date), region)
order by activity_month desc;
