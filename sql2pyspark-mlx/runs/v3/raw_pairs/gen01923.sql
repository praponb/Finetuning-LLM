select year(signup_date) as signup_year, sum(case when city = 'New York' then credit_limit else 0 end) as new_york_credit_limit, sort_array(collect_set(country)) as country_list
from customers
where segment is not null and name not like 'Al%'
group by 1
