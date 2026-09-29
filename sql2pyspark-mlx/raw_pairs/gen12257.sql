select status
from orders
where shipping_fee >= 34
except
select status
from payments
