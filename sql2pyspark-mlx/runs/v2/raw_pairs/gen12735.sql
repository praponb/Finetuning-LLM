select
    transactions.id,
    transactions.is_flagged,
    transactions.txn_type,
    accounts.status,
    accounts.balance,
    customers.signup_date
from transactions
left join accounts on transactions.account_id = accounts.id
right join customers on accounts.customer_id = customers.id
where accounts.account_type = 'CHECKING'
order by accounts.status desc
