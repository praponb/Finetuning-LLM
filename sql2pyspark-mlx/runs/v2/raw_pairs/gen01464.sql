select amount, case when txn_type = 'DEBIT' then 1 else 0 end is_debit
from transactions
where account_id in (4, 8, 34);
