select cost
from products
intersect
select amount as cost
from transactions
where amount between 1500 and 2400
order by cost;
