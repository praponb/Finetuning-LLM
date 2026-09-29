select
    s.id,
    s.browser,
    s.device,
    s.customer_id
from sessions s
where s.customer_id not in (select id from customers where age <> 57);
