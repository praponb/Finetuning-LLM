select * from (select customer_id, account_type, balance from accounts) pivot (sum(balance) for account_type in ('CHECKING', 'SAVINGS', 'LOAN', 'CREDIT'));
