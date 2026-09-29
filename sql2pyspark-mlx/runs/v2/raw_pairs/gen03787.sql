SELECT id, account_type, opened_date FROM (SELECT *, ROW_NUMBER() OVER (PARTITION BY account_type ORDER BY opened_date DESC, id) row_num FROM accounts) t WHERE row_num <= 3
