select a.id, a.status
from accounts a
where a.customer_id in (select id from customers where lower(trim(name)) = 'henry ford');
