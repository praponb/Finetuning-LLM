select id, order_id, amount
from payments
where (amount < 450 and method != 'paypal') or amount >= 1370
