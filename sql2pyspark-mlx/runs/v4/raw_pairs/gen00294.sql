SELECT balance, CASE WHEN account_type IN ('CREDIT', 'LOAN') THEN 'Priority' ELSE 'Other' END AS account_type_group FROM accounts WHERE account_type = 'CHECKING' ORDER BY balance
