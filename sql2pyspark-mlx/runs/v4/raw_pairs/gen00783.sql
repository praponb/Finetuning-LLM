SELECT id, status, account_type, opened_date, DENSE_RANK() OVER (PARTITION BY status, account_type ORDER BY opened_date) AS dense_rnk FROM accounts WHERE balance > 55000;
