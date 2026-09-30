select
    t.id,
    t.txn_type,
    t.is_flagged,
    a.status,
    a.balance,
    a.opened_date
from transactions t
left join accounts a on t.account_id = a.id
where a.account_type not in ('CREDIT', 'LOAN', 'CHECKING')
order by status;
