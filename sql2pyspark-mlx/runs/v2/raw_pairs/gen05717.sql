select sum(case when city = 'Berlin' then 1 else 0 end) berlin_count, count(distinct credit_limit) unique_countrys
from customers
where email like '%@outlook.com'
