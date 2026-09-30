select city
from customers
except
select city
from suppliers
where city != 'Bangkok'
order by city desc;
