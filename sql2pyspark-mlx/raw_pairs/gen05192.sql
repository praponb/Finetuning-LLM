select
    transactions.id,
    transactions.txn_type,
    transactions.txn_time,
    transactions.is_flagged
from transactions
where transactions.account_id in (select id from accounts where opened_date < '2023-03-01');
