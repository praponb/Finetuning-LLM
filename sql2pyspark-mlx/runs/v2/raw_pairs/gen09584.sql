select * from (select account_type, status, balance from accounts) pivot (max(balance) for status in ('FROZEN', 'CLOSED'));
