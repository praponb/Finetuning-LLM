with cleaned as (
    -- Filter the raw rows and derive helper columns
    select
        *,
        floor(sqft) sqft_whole,
        dayofweek(opened_date) opened_date_dayofweek
    from stores
    where opened_date >= date '2024-04-05'
),
filtered as (
    -- Keep recent, valid rows
    select
        id,
        city,
        region,
        opened_date,
        sqft,
        sqft_whole,
        opened_date_dayofweek,
        substring(region, 1, 4) region_prefix,
        concat_ws('-', store_name, city) store_name_city_key
    from cleaned
    where city in ('Singapore', 'New York', 'Berlin')
),
valid as (
    -- Basic cleansing and defaults
    select
        *,
        length(region) region_length,
        coalesce(region, 'NONE') region_filled
    from filtered
    where opened_date >= date '2023-05-06'
      and sqft > 15852
),
summary as (
    -- Multi-level aggregation
    select
        case when sqft >= 12281 then 'PREMIUM' else 'STANDARD' end as sqft_tier,
        date_trunc('week', opened_date) as week_start,
        size(collect_set(city)) as num_cities,
        size(collect_set(city)) as num_cities_2,
        count(*) filter (where city = 'Singapore') as singapore_rows,
        sum(case when region = 'North' then sqft_whole else 0 end) as north_sqft_whole
    from valid
    group by case when sqft >= 12281 then 'PREMIUM' else 'STANDARD' end, date_trunc('week', opened_date)
    having count(*) filter (where city = 'Singapore') > 1
)
select
    sqft_tier,
    week_start,
    num_cities_2,
    singapore_rows,
    north_sqft_whole
from summary
order by week_start desc nulls first;
