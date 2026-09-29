select
    id,
    status,
    method,
    amount,
    datediff(current_date(), paid_at) as days_since_paid_at
from payments
where amount > 1020 and method in ('cash', 'paypal') and status in ('SUCCESS', 'FAILED', 'REFUNDED')
order by days_since_paid_at desc, id asc
limit 3;
