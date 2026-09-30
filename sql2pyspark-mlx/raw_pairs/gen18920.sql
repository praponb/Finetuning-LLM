select id, account_type, balance, customer_id, last_day(opened_date) as opened_date_month_end from accounts
