select sum(case when status = 'REFUNDED' then 1 else 0 end) as refunded_count
from payments
