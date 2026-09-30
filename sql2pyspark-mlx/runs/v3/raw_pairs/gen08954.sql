select
    id,
    amount,
    txn_type,
    account_id
from transactions
where amount > (select avg(amount) from transactions);
