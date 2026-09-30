select city
from customers
union all
select city
from stores
order by city;
