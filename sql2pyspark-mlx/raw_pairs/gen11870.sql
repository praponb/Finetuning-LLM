select
    o.id,
    o.store_id,
    o.amount,
    o.customer_id
from orders o
where o.store_id in (4, 13) and exists (select 1 from shipments s where s.order_id = o.id and s.delivered_date between '2024-06-01' and '2024-10-21');
