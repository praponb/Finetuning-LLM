select quarter(paid_at) as paid_quarter, max(paid_at) as last_paid_at, sum(amount) as total_amount
from payments
group by quarter(paid_at)
