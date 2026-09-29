select
    o.id,
    o.channel,
    o.order_date,
    o.customer_id
from orders o
where o.discount between 0.18 and 0.33 and o.id in (select order_id from payments where status != 'FAILED')
order by 4 desc;
