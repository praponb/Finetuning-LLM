select status
from orders
union all
select status
from payments
where paid_at >= '2024-10-26';
