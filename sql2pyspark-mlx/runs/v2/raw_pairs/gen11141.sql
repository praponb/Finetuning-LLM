select city, country
from customers
where age >= 54
union
select city, country
from suppliers;
