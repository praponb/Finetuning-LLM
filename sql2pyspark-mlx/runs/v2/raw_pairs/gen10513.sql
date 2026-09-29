select txn_time, date_trunc('WEEK', txn_time) as txn_time_period, initcap(trim(txn_type)) as txn_type_title
from transactions
order by txn_time desc
