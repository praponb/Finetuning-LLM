select a.status, a.customer_id, c.is_active
from accounts a
join customers c on a.customer_id = c.id
where a.account_type in ('CREDIT', 'SAVINGS');
