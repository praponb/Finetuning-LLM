select txn_type, sum(case when merchant = 'Amazon' then 1 else 0 end) as amazon_count
from transactions
group by txn_type
order by txn_type;
