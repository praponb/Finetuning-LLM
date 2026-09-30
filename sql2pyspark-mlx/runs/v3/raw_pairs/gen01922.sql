select account_id, sum(amount) as total_amount, rank() over (order by sum(amount) desc) as amount_rank
from transactions
group by account_id
order by 2, account_id
limit 10
