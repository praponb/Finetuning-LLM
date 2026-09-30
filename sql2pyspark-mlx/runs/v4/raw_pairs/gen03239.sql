select id, city
from customers
intersect all
select id, city
from customers
where segment in ('Consumer', 'SMB', 'Corporate')
