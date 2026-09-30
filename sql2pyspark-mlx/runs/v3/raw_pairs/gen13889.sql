select
    subscriptions.id,
    subscriptions.start_date,
    subscriptions.customer_id,
    customers.credit_limit
from subscriptions
inner join customers on subscriptions.customer_id = customers.id
