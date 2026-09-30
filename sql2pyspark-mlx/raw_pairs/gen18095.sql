select quarter(txn_time) as txn_quarter, round(stddev(amount), 2) as stddev_amount, round(avg(amount), 2) as avg_amount
from transactions
group by quarter(txn_time)
