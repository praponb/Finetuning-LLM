select
    a.status,
    a.customer_id,
    a.opened_date,
    t.amount,
    t.is_flagged
from accounts a
left join transactions t on a.id = t.account_id
order by customer_id desc
