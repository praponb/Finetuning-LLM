select
    id,
    amount,
    status,
    method,
    round(amount, 0) as amount_rounded,
    concat(method, '/', status) as method_status
from payments
where order_id = 28
order by 6
