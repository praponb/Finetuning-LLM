select id, txn_type, date_format(txn_time, 'yyyy-MM-dd') as txn_time_label
from transactions
where txn_type = 'CREDIT';
