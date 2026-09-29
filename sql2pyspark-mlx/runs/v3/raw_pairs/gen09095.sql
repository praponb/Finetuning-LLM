select city, country
from customers
intersect
select city, country
from suppliers;
