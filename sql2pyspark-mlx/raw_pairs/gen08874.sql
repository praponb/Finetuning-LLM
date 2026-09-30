select
    accounts.id,
    accounts.status,
    accounts.balance,
    customers.country
from accounts
join customers on accounts.customer_id = customers.id
where customers.segment in ('SMB', 'Corporate')
order by status
