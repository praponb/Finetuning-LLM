select a.id, a.account_type
from accounts a
where a.opened_date <= '2024-09-01' and exists (select 1 from transactions t where t.account_id = a.id);
