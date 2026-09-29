select id, balance, account_type, date_trunc('year', opened_date) as opened_date_period from accounts where month(opened_date) in (2, 4, 5)
