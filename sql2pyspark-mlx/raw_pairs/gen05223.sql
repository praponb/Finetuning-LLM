select a.id, a.customer_id, a.balance
from accounts a
where not exists (select 1 from transactions t where t.account_id = a.id and t.merchant in ('Amazon', 'Starbucks', '7-Eleven'))
order by a.customer_id, id desc;
