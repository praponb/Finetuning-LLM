select txn_type, sum(case when merchant = 'Grab' then amount else 0 end) as grab_amount, max(amount) as max_amount
from transactions
group by txn_type;
