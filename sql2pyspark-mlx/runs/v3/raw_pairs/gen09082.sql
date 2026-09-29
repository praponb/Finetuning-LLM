select * from (select account_type, status, balance from accounts)
pivot (avg(balance) for status in ('OPEN', 'FROZEN'))
