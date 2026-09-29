with kept_keys as (
    -- Set difference on keys
    select city, id
    from stores
    except
    select city, id
    from stores
    where region in ('South', 'Central')
),
verified as (
    -- Keep rows whose keys survive the set operation
    select s.*
    from stores s
    inner join kept_keys k on s.city = k.city and s.id = k.id
),
kept as (
    -- Anti-join via a correlated subquery
    select s.*
    from verified s
    where not exists (
            select 1
            from store_sales x
            where x.store_id = s.id
              and x.sale_date <= s.opened_date
        )
),
not_flagged as (
    -- Anti-join via a correlated subquery
    select s.*
    from kept s
    where not exists (
            select 1
            from inventory ch
            where ch.store_id = s.id
              and ch.last_restock <= s.opened_date
        )
),
remaining_keys as (
    -- Set difference on keys
    select id
    from not_flagged
    except
    select id
    from stores
    where region is not null
),
reconciled as (
    -- Keep rows whose keys survive the set operation
    select s.*
    from not_flagged s
    inner join remaining_keys k on s.id = k.id
),
summary as (
    -- Multi-level aggregation
    select
        date_trunc('year', opened_date) as year_start,
        region,
        sum(case when region = 'North' then sqft else 0 end) as north_sqft,
        percentile_approx(sqft, array(0.25, 0.5, 0.75)) as sqft_quartiles,
        max(sqft) as max_sqft,
        bit_or(sqft) as sqft_bit_or,
        rank() over (partition by date_trunc('year', opened_date) order by max(sqft) desc) as max_sqft_rank
    from reconciled
    group by date_trunc('year', opened_date), region
    having sqft_bit_or < 1000
)
select
    year_start,
    region,
    sqft_bit_or,
    case when region rlike '^(Central|East)$' then 'OTHER_ZONE' when region rlike '^(North|South)$' then 'PRIORITY' else 'CORE' end as region_zone
from summary;
