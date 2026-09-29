SELECT *, INITCAP(TRIM(account_type)) AS account_type_title FROM accounts WHERE status <> 'CLOSED'
