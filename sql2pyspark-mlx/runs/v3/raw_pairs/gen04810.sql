SELECT id, customer_id, account_type, balance, ADD_MONTHS(opened_date, 1) AS opened_date_plus_months, COALESCE(balance, 0) AS balance_filled FROM accounts WHERE status = 'FROZEN'
