select
    customers.segment,
    customers.name,
    customers.country,
    accounts.balance,
    accounts.opened_date
from customers
left join accounts on accounts.customer_id = customers.id
order by country;
