select store_name
from stores
intersect
select country
from suppliers
order by store_name
