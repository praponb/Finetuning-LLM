select country
from customers
where segment in ('SMB', 'Corporate')
intersect
select country
from customers
where city in ('Berlin', 'Chiang Mai');
