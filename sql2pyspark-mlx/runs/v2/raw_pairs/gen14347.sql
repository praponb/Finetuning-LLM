select id, customer_id
from accounts
where balance <= 39000
union all
select id, customer_id
from accounts
where balance between 66000 and 72000
order by id;
