select id, status
from accounts
where customer_id is not null
union all
select id, status
from accounts
where balance >= 20000;
