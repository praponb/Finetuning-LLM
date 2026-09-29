select
    t.txn_type,
    t.merchant,
    t.is_flagged,
    a.status,
    case when t.txn_type in ('CREDIT', 'DEBIT') then 'Priority' else 'Other' end as txn_type_group
from transactions t
inner join accounts a on a.id = t.account_id
order by merchant, txn_type
