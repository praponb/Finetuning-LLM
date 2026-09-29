select
    a.id,
    a.account_type,
    a.balance,
    a.customer_id
from accounts a
where a.status = 'FROZEN' and not exists (select 1 from transactions t where t.account_id = a.id and not t.is_flagged);
