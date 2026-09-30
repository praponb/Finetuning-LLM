select
    accounts.account_type,
    accounts.opened_date,
    customers.credit_limit,
    customers.city,
    customers.email,
    regexp_extract(customers.email, '@(.+)$', 1) as email_domain
from accounts
left outer join customers on customers.id = accounts.customer_id
order by accounts.opened_date desc;
