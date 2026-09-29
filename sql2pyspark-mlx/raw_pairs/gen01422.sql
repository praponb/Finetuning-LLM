with base as (
    -- Filter the raw rows and derive helper columns
    select *, coalesce(supplier_name, 'N/A') supplier_name_filled
    from suppliers
    where city is not null
),
cleaned as (
    -- Keep recent, valid rows
    select
        id,
        supplier_name,
        country,
        city,
        rating,
        supplier_name_filled,
        concat_ws('-', city, country) city_country_key,
        coalesce(city, 'UNKNOWN') resolved_city
    from base
    where city is not null
),
kept_keys as (
    -- Set difference on keys
    select country, id
    from cleaned
    except
    select country, id
    from suppliers
    where city in ('New York', 'Singapore', 'Chiang Mai')
),
reconciled as (
    -- Keep rows whose keys survive the set operation
    select s.*
    from cleaned s
    inner join kept_keys k on s.country = k.country and s.id = k.id
),
enriched as (
    -- Ranking and gap features
    select
        id,
        supplier_name,
        country,
        city,
        rating,
        city_country_key,
        resolved_city,
        round(rating / sum(rating) over (partition by country), 4) rating_share,
        avg(rating) over (partition by city) city_avg_rating
    from reconciled
    where supplier_name like 'Eur%'
)
-- Aggregate with conditional metrics
select
    case when rating >= 3.6 then 'PREMIUM' else 'STANDARD' end as rating_tier,
    country,
    size(collect_set(country)) as num_countries,
    count(distinct id) as distinct_ids,
    size(collect_set(country)) as num_countries_2,
    count(*) filter (where country = 'JP') as jp_rows
from enriched
group by grouping sets ((case when rating >= 3.6 then 'PREMIUM' else 'STANDARD' end, country), (case when rating >= 3.6 then 'PREMIUM' else 'STANDARD' end), ())
having count(*) filter (where country = 'JP') > 1
order by num_countries_2 desc;
