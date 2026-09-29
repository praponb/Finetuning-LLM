select
    t.id,
    t.merchant,
    t.account_id,
    t.txn_time
from transactions t
where t.txn_type in ('DEBIT', 'FEE') and t.account_id not in (select id from accounts where opened_date < '2024-04-01')
