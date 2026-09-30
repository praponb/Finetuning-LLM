select id, account_id
from transactions
where not (amount < 3500 and account_id in (1, 3, 7, 21) and hour(txn_time) between 6 and 14);
