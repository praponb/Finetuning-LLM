select id, status, balance, round(balance, 1) as balance_rounded, upper(status) as status_upper from accounts where account_type in ('CREDIT', 'SAVINGS', 'LOAN') and customer_id is null
