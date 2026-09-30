select txn_type, round(stddev(amount), 2) as stddev_amount
from transactions
group by txn_type
having stddev_amount < 68
order by 1
