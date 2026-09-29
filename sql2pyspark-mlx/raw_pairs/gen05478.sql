select country, city
from suppliers
except distinct
select country, city
from suppliers
where city <> 'New York'
