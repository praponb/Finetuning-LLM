select
    o.id,
    o.customer_id,
    o.channel,
    o.amount
from orders o
where exists (select 1 from shipments s where s.order_id = o.id and s.delivered_date <= '2023-04-10');
