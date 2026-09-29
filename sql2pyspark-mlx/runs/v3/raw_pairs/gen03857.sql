select merchant, txn_type, min(amount) as min_amount
from transactions
group by merchant, txn_type
having min_amount >= 1300
order by merchant
