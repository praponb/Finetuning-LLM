select
    t.id,
    t.merchant,
    t.txn_type,
    a.status,
    year(a.opened_date) as opened_date_year
from transactions t
inner join accounts a on t.account_id = a.id
where t.amount < 1100
order by 4
