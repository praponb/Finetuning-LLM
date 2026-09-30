select
    hour(txn_time) txn_hour,
    to_date(txn_time) txn_day,
    merchant,
    count(distinct amount) unique_merchants,
    count(*) total_count
from transactions
group by txn_hour, txn_day, merchant
order by unique_merchants desc, txn_day
