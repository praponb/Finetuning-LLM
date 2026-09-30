select * from (select account_type, status, balance from accounts where status = 'CLOSED') pivot (count(balance) for status in ('CLOSED', 'OPEN', 'FROZEN')) order by account_type
