select
    customers.credit_limit,
    customers.segment,
    sessions.country,
    sessions.customer_id,
    sessions.device,
    upper(customers.segment) as segment_upper
from customers
join sessions on sessions.customer_id = customers.id;
