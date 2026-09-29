select id, status, round(balance, 0) as balance_rounded from accounts where account_type not in ('LOAN', 'SAVINGS') and account_type = 'CHECKING'
