SELECT id, account_type, opened_date, ROW_NUMBER() OVER (PARTITION BY account_type ORDER BY opened_date, id) AS opened_date_row FROM accounts;
