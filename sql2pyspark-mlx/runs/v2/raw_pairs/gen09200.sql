select order_id, status, cast(amount as int) amount_int
from payments
where status <> 'REFUNDED' and method = 'promptpay'
order by status, amount_int desc;
