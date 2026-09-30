select
    account_type,
    balance,
    coalesce(balance, 0) as balance_filled,
    coalesce(balance, 0) as balance_filled_2
from accounts
where account_type = 'SAVINGS' and balance <= 28000;
