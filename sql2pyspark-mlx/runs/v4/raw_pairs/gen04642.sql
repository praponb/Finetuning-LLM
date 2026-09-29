with filtered as (
    -- Filter the raw rows and derive helper columns
    select
        *,
        case when city = 'Bangkok' then 1 else 0 end is_bangkok,
        coalesce(city, 'UNKNOWN') city_filled
    from suppliers
    where city is not null
      and rating is null
),
not_flagged as (
    -- Anti-join via a correlated subquery
    select s.*
    from filtered s
    where not exists (
            select 1
            from products x
            where x.supplier_id = s.id
              and x.stock_qty <> 192
        )
),
kept_keys as (
    -- Set intersection on keys
    select city
    from not_flagged
    intersect
    select city
    from suppliers
    where city in ('Singapore', 'Berlin', 'Tokyo')
),
verified as (
    -- Keep rows whose keys survive the set operation
    select s.*
    from not_flagged s
    inner join kept_keys k on s.city = k.city
),
base as (
    -- Filter the raw rows and derive helper columns
    select
        *,
        concat_ws(',', supplier_name, country) supplier_name_country_key,
        coalesce(city, 'UNKNOWN') resolved_city
    from verified
    where city <> 'London'
),
with_windows as (
    -- Running and moving aggregates
    select
        *,
        stddev_samp(is_bangkok) over (partition by city, country) city_stddev_samp_is_bangkok,
        dense_rank() over (partition by city order by is_bangkok desc nulls last) is_bangkok_dense_rank,
        rank() over (partition by city order by is_bangkok asc nulls first) is_bangkok_rank
    from base
)
-- Aggregate with conditional metrics
select
    city,
    case when rating >= 3.8 then 'HIGH' else 'LOW' end as rating_tier,
    max(is_bangkok_dense_rank) filter (where country = 'DE') as de_max_is_bangkok_dense_rank,
    count(*) filter (where country = 'TH') as th_rows,
    count(*) as event_count,
    sort_array(collect_set(country)) as country_list
from with_windows
group by city, case when rating >= 3.8 then 'HIGH' else 'LOW' end
order by th_rows asc nulls last;
