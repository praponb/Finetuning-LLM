select order_id, paid_at, case when status = 'SUCCESS' then 1 else 0 end is_success
from payments
