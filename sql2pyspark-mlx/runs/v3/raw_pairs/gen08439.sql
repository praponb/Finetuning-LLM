select status, sum(amount) as total_amount, rank() over (order by sum(amount) desc) as amount_rank
from payments
where order_id = 26
group by status
order by amount_rank desc, status
