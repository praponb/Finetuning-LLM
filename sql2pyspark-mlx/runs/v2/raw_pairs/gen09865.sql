select a.id, a.account_type
from accounts a
where a.customer_id in (select id from customers where year(signup_date) >= 2024)
