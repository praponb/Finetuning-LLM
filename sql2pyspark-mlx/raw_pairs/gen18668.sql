select country, city
from suppliers
where rating < 3.8
intersect
select country, city
from suppliers
where supplier_name is not null
order by city
