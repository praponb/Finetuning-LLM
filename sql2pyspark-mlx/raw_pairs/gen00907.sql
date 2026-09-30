select country
from customers
where country not in ('TH', 'SG')
intersect
select country
from sessions;
