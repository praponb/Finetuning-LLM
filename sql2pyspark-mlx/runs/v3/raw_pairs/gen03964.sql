select is_active, count(distinct credit_limit) as unique_citys
from customers
where city in ('London', 'Singapore')
group by is_active
order by unique_citys desc
