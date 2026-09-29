select a.id, a.account_type
from accounts a
where a.account_type = 'CREDIT' and a.customer_id in (select id from customers where not is_active)
order by a.account_type, id
limit 10;
