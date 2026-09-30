select
    accounts.status,
    accounts.customer_id,
    accounts.account_type,
    transactions.txn_type,
    transactions.is_flagged
from accounts
join transactions on accounts.id = transactions.account_id
where accounts.opened_date between '2025-03-25' and '2025-07-01'
