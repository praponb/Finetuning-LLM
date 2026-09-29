select *, length(account_type) as account_type_length from accounts where account_type not in ('CREDIT', 'LOAN', 'CHECKING') and account_type in ('SAVINGS', 'CHECKING');
