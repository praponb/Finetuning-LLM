select a.id, a.opened_date, a.account_type
from accounts a
where a.customer_id in (select id from customers where signup_date >= '2023-06-25');
