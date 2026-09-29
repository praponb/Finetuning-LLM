select
    a.id,
    a.customer_id,
    a.opened_date,
    a.balance
from accounts a
where a.account_type <> 'CREDIT' and a.customer_id not in (select id from customers where is_active = true)
order by balance desc
