select
    t.txn_time,
    t.merchant,
    t.is_flagged,
    a.status,
    a.customer_id,
    a.account_type
from transactions t
join accounts a on a.id = t.account_id;
