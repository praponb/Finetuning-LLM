select
    status,
    order_id,
    method,
    count(case when status = 'REFUNDED' then 1 end) as refunded_count
from payments
where order_id in (20, 21) and amount between 600 and 1340
group by status, order_id, method
having count(case when status = 'REFUNDED' then 1 end) >= 4;
