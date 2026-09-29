select
    t.id,
    t.account_id,
    a.status,
    a.balance
from transactions t
join accounts a on t.account_id = a.id;
