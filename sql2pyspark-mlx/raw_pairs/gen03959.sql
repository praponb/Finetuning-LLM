select city, 'customer' as kind
from customers
union all
select city, 'store' as kind
from stores
where region is null
