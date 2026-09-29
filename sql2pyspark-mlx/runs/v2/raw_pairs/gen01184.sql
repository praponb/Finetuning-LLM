select *, concat_ws('/', method, status) as method_status_key, case when status = 'FAILED' then 1 else 0 end as is_failed
from payments
where amount <= 1500;
