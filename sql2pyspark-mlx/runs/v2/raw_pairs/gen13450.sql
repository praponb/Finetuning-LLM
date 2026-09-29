select city
from customers
intersect
select city
from stores
where region not in ('West', 'North')
order by city
