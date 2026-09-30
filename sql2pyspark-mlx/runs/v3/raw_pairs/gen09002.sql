select status
from orders
where amount > 1130
except
select status
from payments
where amount >= 1170;
