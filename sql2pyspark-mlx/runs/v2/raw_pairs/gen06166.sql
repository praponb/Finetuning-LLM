select
    a.id,
    a.customer_id,
    a.opened_date,
    a.balance
from accounts a
where a.customer_id in (select id from customers where country in ('JP', 'TH', 'US'));
