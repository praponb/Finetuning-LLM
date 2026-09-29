select
    id,
    status,
    paid_at,
    coalesce(method, 'N/A') as method_filled
from payments
