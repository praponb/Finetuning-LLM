select
    accounts.balance,
    accounts.opened_date,
    accounts.account_type,
    customers.email,
    customers.name
from accounts
inner join customers on accounts.customer_id = customers.id
where accounts.account_type not in ('SAVINGS', 'CREDIT');
