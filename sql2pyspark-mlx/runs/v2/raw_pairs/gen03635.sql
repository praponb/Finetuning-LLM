select
    id,
    amount,
    txn_time,
    account_id
from transactions
where amount >= (select max(amount) from transactions);
