select order_id, method, count(case when status = 'REFUNDED' then 1 end) as refunded_count
from payments
group by order_id, method
order by order_id desc;
