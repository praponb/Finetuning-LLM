select city
from suppliers
where rating <= 3.1
union
select city
from stores
where region in ('West', 'South', 'East')
order by city
