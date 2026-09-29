select
    date_trunc('year', signup_date) as year_start,
    case when age >= 68 then 'HIGH' else 'LOW' end as age_tier,
    size(collect_set(country)) as num_countries,
    count(case when city = 'Chiang Mai' then 1 end) as chiang_mai_count,
    approx_percentile(credit_limit, 0.9) as credit_limit_p90,
    sort_array(collect_set(city)) as city_list,
    max(age) filter (where segment = 'SMB') as smb_max_age
from customers
group by 1, 2
order by chiang_mai_count;
