select status, sum(discount) as total_discount, sum(sum(discount)) over (order by status) as cumulative_discount
from orders
group by status
order by status, total_discount desc;
