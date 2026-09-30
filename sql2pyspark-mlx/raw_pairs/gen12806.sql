select
    id,
    status,
    amount,
    trim(method) as clean_method
from payments
