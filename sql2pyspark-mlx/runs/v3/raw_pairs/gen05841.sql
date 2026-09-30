select amount, status
from payments
where not (order_id in (20, 28, 38) and amount >= 940)
order by status desc, amount desc
