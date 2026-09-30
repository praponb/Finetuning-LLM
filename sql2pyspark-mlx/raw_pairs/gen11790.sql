select
    order_id,
    sum(amount) as total_amount,
    count(*) as cnt,
    rank() over (order by sum(amount) desc) as rnk
from payments
group by order_id
order by rnk
