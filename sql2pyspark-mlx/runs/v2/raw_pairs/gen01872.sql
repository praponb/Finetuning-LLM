select
    method,
    round(avg(amount), 2) as avg_amount,
    max(paid_at) as last_paid_at,
    max(amount) - min(amount) as amount_range
from payments
where to_date(paid_at) < '2023-11-01'
group by 1;
