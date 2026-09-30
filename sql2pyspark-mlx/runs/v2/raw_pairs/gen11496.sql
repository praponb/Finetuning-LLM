select * from (select account_type, status, balance from accounts)
pivot (count(balance) for status in ('FROZEN', 'CLOSED', 'OPEN'))
