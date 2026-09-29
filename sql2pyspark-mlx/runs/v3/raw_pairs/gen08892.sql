select
    id,
    account_type,
    status,
    balance,
    coalesce(status, 'Unknown') as status_filled
from accounts
where account_type in ('CHECKING', 'SAVINGS', 'LOAN') or month(opened_date) in (1, 2, 7)
order by status_filled asc;
