select 'customer' as origin, city, country
from customers
where country = 'DE'
union
select 'supplier' as origin, city, country
from suppliers;
