select
    id,
    credit_limit,
    name,
    substring(name, 1, 2) as name_prefix,
    trim(segment) as clean_segment
from customers
where not is_active
