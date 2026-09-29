select country, city
from customers
where name like 'P%'
union
select country, city
from suppliers;
