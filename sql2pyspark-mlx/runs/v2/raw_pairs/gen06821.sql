select merchant, quarter(txn_time) as txn_quarter, avg(amount) as avg_amount
from transactions
where to_date(txn_time) < '2023-04-18' and account_id = 33 and amount < 1700
group by merchant, quarter(txn_time)
having count(case when merchant = 'Grab' then 1 end) > 3
order by txn_quarter desc, avg_amount desc;
