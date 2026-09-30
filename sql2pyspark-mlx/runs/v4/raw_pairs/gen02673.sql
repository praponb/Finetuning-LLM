select
    a.id,
    a.opened_date,
    a.balance,
    a.customer_id
from accounts a
where a.account_type <> 'SAVINGS' and a.customer_id in (select id from customers where country in ('US', 'UK'))
order by balance desc
