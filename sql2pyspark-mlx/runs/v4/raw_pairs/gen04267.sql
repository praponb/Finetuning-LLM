select order_id, year(paid_at) as paid_year, max(amount) - min(amount) as amount_range
from payments
group by order_id, year(paid_at)
order by order_id desc
