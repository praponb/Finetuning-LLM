select
    id,
    order_id,
    status,
    method,
    substring(status, 1, 4) as status_prefix
from payments
where order_id is null and status in ('SUCCESS', 'FAILED');
