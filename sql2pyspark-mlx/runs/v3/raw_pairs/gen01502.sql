select country, sum(case when city = 'Tokyo' then age else 0 end) as tokyo_age, count(distinct credit_limit) as unique_countrys
from customers
where country = 'TH'
group by 1
having sum(case when city = 'Tokyo' then age else 0 end) > 62
order by unique_countrys, country
limit 20;
