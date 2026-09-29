select * from (select account_type, status, balance from accounts where status in ('FROZEN', 'CLOSED')) pivot (sum(balance) for status in ('FROZEN', 'OPEN')) order by account_type
