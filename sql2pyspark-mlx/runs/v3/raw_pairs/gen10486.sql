select
    t.id,
    t.account_id,
    a.customer_id,
    a.opened_date
from transactions t
left join accounts a on a.id = t.account_id
where t.txn_type = 'DEBIT' and a.status != 'OPEN'
order by t.account_id;
