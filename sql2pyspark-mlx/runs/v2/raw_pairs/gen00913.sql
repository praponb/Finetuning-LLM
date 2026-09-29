select id, amount, case when status = 'SUCCESS' then 1 else 0 end as is_success
from payments
where amount >= 1320 and status = 'REFUNDED';
