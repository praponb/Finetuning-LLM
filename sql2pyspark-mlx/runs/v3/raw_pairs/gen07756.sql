select id, balance, opened_date, concat(account_type, ', ', status) as account_type_status from accounts where customer_id = 10 or status <> 'OPEN'
