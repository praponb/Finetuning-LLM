with remaining_keys as (
    select city, segment
    from customers
    intersect all
    select city, segment
    from customers
    where name is null
),
reconciled as (
    select s.*
    from customers s
    inner join remaining_keys k on s.city = k.city and s.segment = k.segment
)
select
    country,
    case when age >= 67 then 'HIGH' else 'LOW' end as age_tier,
    sort_array(collect_set(city)) as city_list,
    size(collect_set(segment)) as num_segments,
    max(credit_limit) as max_credit_limit
from reconciled
group by country, age_tier
having max_credit_limit > 0;
