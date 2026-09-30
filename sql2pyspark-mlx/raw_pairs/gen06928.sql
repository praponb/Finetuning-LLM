select store_id
from orders
except all
select store_id
from orders
where order_date < '2025-03-01';
