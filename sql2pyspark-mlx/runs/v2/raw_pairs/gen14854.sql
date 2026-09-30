select
    t.id,
    t.merchant,
    t.account_id,
    t.amount,
    a.status,
    a.customer_id,
    a.balance,
    abs(a.balance) abs_balance
from transactions t
right join accounts a on t.account_id = a.id
where t.merchant = 'Shell' and a.account_type in ('SAVINGS', 'CHECKING')
order by amount
