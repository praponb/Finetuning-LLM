select amount
from payments
where amount is not null
except
select price as amount
from products
where price is not null
order by amount
