select
    t.id,
    t.amount,
    t.txn_time,
    t.txn_type
from transactions t
where t.account_id not in (select id from accounts where account_type not in ('SAVINGS', 'CREDIT', 'CHECKING'))
