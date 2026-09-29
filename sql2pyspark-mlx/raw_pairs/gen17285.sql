select
    a.id,
    a.opened_date,
    a.account_type,
    a.customer_id
from accounts a
where a.id in (select account_id from transactions)
order by opened_date desc
