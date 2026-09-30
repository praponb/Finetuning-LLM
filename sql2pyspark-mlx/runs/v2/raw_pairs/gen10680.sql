select c.id, c.city, c.country
from customers c
where not exists (select 1 from accounts a where a.customer_id = c.id and a.account_type in ('CHECKING', 'LOAN', 'SAVINGS'));
