select merchant, sum(amount) as total_amount
from transactions
where account_id in (10, 12, 19) or is_flagged
group by merchant
