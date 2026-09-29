select
    s.id,
    s.device,
    s.country,
    s.customer_id
from sessions s
where s.customer_id not in (select id from customers where city = 'Berlin');
