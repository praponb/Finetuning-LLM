select
    credit_limit,
    is_active,
    city,
    upper(trim(name)) as name_normalized
from customers
