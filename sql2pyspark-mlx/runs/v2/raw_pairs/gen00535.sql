select
    id,
    paid_at,
    upper(method) as method_upper,
    to_date(paid_at) as paid_at_date
from payments
where method in ('paypal', 'cash');
