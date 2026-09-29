select a.id, a.status, a.customer_id
from accounts a
where a.balance >= 79000 and a.id in (select account_id from transactions)
order by id
limit 20;
