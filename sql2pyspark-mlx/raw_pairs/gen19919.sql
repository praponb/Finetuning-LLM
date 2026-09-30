select status as label
from payments
where amount >= 1150
intersect
select segment as label
from customers
