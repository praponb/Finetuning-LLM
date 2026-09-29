select a.id, a.account_type
from accounts a
where a.customer_id in (19, 25, 34, 38) and not exists (select 1 from transactions t where t.account_id = a.id and t.merchant <> 'Shell')
order by account_type
