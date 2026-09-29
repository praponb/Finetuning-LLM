select
    t.is_flagged,
    t.merchant,
    t.account_id,
    a.balance,
    a.status,
    a.account_type
from transactions t
left join accounts a on a.id = t.account_id
where a.balance between 43000 and 65000 and t.account_id in (31, 32);
