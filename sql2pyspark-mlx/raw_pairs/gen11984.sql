SELECT id, account_type, balance, opened_date, status FROM accounts QUALIFY ROW_NUMBER() OVER (PARTITION BY customer_id, account_type ORDER BY balance NULLS FIRST, id) = 1
