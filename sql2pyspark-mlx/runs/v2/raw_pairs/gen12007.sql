SELECT id, customer_id, account_type, DATE_TRUNC('YEAR', opened_date) AS opened_date_period, COALESCE(balance, 0) AS balance_filled FROM accounts;
