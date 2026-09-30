select
    id,
    method,
    concat_ws(',', status, method) as status_method_key,
    upper(status) as status_upper
from payments
where status in ('REFUNDED', 'FAILED') or order_id in (26, 30);
