select
    t.amount,
    a.opened_date,
    a.balance,
    a.customer_id
from transactions t
left join accounts a on t.account_id = a.id
where a.status in ('CLOSED', 'OPEN')
order by amount desc, opened_date
