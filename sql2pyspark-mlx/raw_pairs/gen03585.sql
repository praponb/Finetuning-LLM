select
    t.txn_type,
    a.opened_date,
    a.account_type,
    coalesce(a.status, 'N/A') as status_filled
from transactions t
inner join accounts a on t.account_id = a.id;
