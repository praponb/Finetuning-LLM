with valid as (
    select
        *,
        upper(trim(supplier_name)) as supplier_name_normalized,
        replace(supplier_name, ' ', '') as supplier_name_slug
    from suppliers
    where city is not null
      and rating between 1.9 and 4.0
),
matched_keys as (
    select country
    from valid
    except all
    select country
    from suppliers
    where rating between 2.7 and 3.9
),
reconciled as (
    select s.*
    from valid s
    inner join matched_keys k on s.country = k.country
),
not_flagged as (
    select s.*
    from reconciled s
    where not exists (
            select 1
            from products x
            where x.supplier_id = s.id
              and x.stock_qty >= 298
        )
),
base as (
    select
        *,
        initcap(trim(country)) as country_title,
        lower(supplier_name) as supplier_name_lower
    from not_flagged
    where city is not null
      and country in ('UK', 'DE', 'TH')
),
base_suppliers as (
    select
        id,
        supplier_name,
        country,
        city,
        supplier_name_normalized,
        supplier_name_lower,
        substring(city, 1, 3) as city_prefix,
        coalesce(city, 'N/A') as city_filled
    from base
    where city is not null
      and rating <= 2.9
)
select
    country,
    city,
    count(distinct city) as distinct_cities,
    size(collect_set(country)) as num_countries,
    sort_array(collect_set(country)) as country_list,
    grouping(country) as grouping_id
from base_suppliers
group by rollup (country, city)
having distinct_cities > 1
order by num_countries desc;
