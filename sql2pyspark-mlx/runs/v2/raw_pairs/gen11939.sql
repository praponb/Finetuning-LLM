select
    a.id,
    a.customer_id,
    a.balance,
    a.opened_date
from accounts a
where a.customer_id in (select id from customers where city = 'Chiang Mai')
order by a.balance desc, a.opened_date
