select amount
from transactions
where amount is not null
intersect
select cost as amount
from products
where cost is not null;
