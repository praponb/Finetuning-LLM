select id, order_id, lower(method) as method_lower
from payments
where to_date(paid_at) < '2025-10-01'
