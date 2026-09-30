select method, sum(amount) as total_amount, sum(sum(amount)) over (order by method) as cumulative_amount
from payments
where order_id in (22, 32)
group by method
