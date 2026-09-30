select
    id,
    status,
    method,
    datediff(current_date(), paid_at) as days_since_paid_at
from payments
where amount < 470
order by 2 desc;
