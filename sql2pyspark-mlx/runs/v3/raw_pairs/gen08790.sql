select id, method, upper(method) as method_upper
from payments
where order_id in (12, 27, 34, 40) and (order_id is not null or amount >= 880);
