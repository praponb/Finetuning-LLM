select quarter(txn_time) as txn_quarter, count(case when merchant = 'Amazon' then 1 end) as amazon_count, round(stddev(amount), 2) as stddev_amount
from transactions
group by 1
