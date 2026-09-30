select method, max(paid_at) as last_paid_at
from payments
group by method
order by 1 desc
