select
    subscriptions.id,
    subscriptions.customer_id,
    subscriptions.start_date,
    subscriptions.monthly_fee,
    customers.name,
    customers.credit_limit
from subscriptions
left outer join customers on subscriptions.customer_id = customers.id
where subscriptions.monthly_fee between 80 and 150
order by subscriptions.monthly_fee desc, customer_id;
