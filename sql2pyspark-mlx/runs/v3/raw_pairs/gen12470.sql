select
    case when city in ('Singapore', 'Bangkok') then 'priority' else 'other' end as city_group,
    country,
    is_active,
    sort_array(collect_set(country)) as country_list,
    count(credit_limit) as credit_limit_count,
    avg(credit_limit) as avg_credit_limit
from customers
group by case when city in ('Singapore', 'Bangkok') then 'priority' else 'other' end, country, is_active
order by city_group
