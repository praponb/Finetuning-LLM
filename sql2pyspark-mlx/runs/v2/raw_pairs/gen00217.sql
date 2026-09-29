select
    id,
    order_id,
    concat(status, ' - ', method) status_method,
    ceil(amount) amount_whole
from payments
where amount <= 1030 and order_id in (14, 24, 38)
