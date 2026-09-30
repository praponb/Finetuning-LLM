select is_flagged, round(avg(amount), 2) as avg_amount
from transactions
where txn_type = 'FEE'
group by 1
