with recent_suppliers as (
    select *
    from suppliers
    where country in ('UK', 'US', 'TH') and supplier_name is null
),
agg as (
    select
        city,
        case when rating >= 4.2 then 'high' when rating >= 1.4 then 'mid' else 'low' end as rating_bucket,
        sort_array(collect_set(city)) as city_list,
        count(*) as total_count
    from recent_suppliers
    group by city, case when rating >= 4.2 then 'high' when rating >= 1.4 then 'mid' else 'low' end
)
select
    city,
    rating_bucket,
    city_list,
    total_count
from agg
