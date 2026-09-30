select * from (select account_type, status, balance from accounts where balance < 20000) pivot (avg(balance) for status in ('OPEN', 'CLOSED', 'FROZEN'))
