SELECT opened_date, CASE WHEN account_type IN ('SAVINGS', 'CREDIT') THEN 'Tier 1' ELSE 'Other' END AS account_type_group, SUBSTRING(account_type, 1, 2) AS account_type_prefix
FROM accounts
ORDER BY opened_date
