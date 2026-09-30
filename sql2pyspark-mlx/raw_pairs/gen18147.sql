select a.id, a.account_type, a.balance
from accounts a
where a.customer_id in (23, 26) and a.customer_id in (select id from customers where country != 'US');
