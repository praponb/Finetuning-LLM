select *, to_date(txn_time) as txn_time_date, ceil(amount) as amount_whole
from transactions
where txn_type in ('CREDIT', 'DEBIT') or hour(txn_time) between 10 and 16;
