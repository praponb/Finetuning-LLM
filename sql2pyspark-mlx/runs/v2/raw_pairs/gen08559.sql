select
    customers.credit_limit,
    customers.segment,
    customers.name,
    sessions.customer_id,
    sessions.country
from customers
left join sessions on sessions.customer_id = customers.id
