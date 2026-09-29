select city from customers except select status from accounts where account_type in ('SAVINGS', 'LOAN', 'CHECKING');
