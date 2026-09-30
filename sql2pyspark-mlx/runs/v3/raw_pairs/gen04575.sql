select
    t.id,
    t.is_flagged,
    t.amount,
    t.merchant
from transactions t
where to_date(t.txn_time) < '2025-04-01' and t.account_id in (select id from accounts where opened_date <= '2023-07-01');
