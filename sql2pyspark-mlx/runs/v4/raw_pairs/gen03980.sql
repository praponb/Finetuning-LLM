select
    date_trunc('month', opened_date) as activity_month,
    sum(sqft) filter (where city = 'Bangkok') as bangkok_total_sqft,
    size(collect_set(city)) as num_cities,
    sum(case when city = 'Bangkok' then sqft else 0 end) as bangkok_sqft,
    round(sum(sqft) / nullif(count(case when region = 'Central' then 1 end), 0), 2) as sqft_per_central,
    count_if(region = 'North') as north_events,
    size(collect_set(city)) as num_cities_2
from stores
group by activity_month;
