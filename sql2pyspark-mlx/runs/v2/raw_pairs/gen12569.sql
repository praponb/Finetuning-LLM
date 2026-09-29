select id, txn_time
from transactions
where merchant in ('Grab', 'Lazada', 'Shell')
order by txn_time desc;
