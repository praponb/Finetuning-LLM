select *
from (
    select
        segment,
        city,
        sort_array(collect_set(country)) country_list,
        max(age) - min(age) age_range
    from customers
    where credit_limit <= 15300
    group by segment, city
) x
where age_range < 23
order by segment;
