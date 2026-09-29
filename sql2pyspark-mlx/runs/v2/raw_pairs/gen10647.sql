select * from (select account_type, status, balance from accounts where customer_id = 18)
pivot (sum(balance) for status in ('OPEN', 'FROZEN', 'CLOSED'))
