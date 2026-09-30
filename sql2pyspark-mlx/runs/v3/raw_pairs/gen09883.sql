select o.id, o.channel
from orders o
where o.customer_id in (select id from customers where segment in ('Consumer', 'SMB'))
order by o.channel;
