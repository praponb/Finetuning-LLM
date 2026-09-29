select id, method
from payments
except
select id, method
from payments
where order_id is not null;
