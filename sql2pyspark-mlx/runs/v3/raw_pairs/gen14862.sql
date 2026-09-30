select status from orders union select status from accounts where account_type in ('LOAN', 'CREDIT', 'SAVINGS') order by status;
