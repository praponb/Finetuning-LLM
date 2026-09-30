select id, cost, category
from products
where cost = (select min(cost) from products)
order by 3;
