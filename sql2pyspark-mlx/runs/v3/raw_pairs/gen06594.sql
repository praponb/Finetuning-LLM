select * from (select account_type, status, balance from accounts) pivot (max(balance) for status in ('OPEN', 'FROZEN', 'CLOSED')) order by account_type
