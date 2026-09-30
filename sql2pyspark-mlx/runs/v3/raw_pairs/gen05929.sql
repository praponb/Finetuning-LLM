select
    transactions.id,
    transactions.merchant,
    accounts.opened_date,
    customers.is_active
from transactions
join accounts on transactions.account_id = accounts.id
join customers on accounts.customer_id = customers.id
order by id desc
limit 5;
