select
    transactions.txn_type,
    accounts.status,
    accounts.customer_id,
    accounts.account_type
from transactions
join accounts on transactions.account_id = accounts.id;
