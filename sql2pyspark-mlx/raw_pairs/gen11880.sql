SELECT *, INITCAP(TRIM(account_type)) AS account_type_title
FROM accounts
WHERE balance IS NOT NULL
