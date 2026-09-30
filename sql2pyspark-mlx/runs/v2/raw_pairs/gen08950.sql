SELECT *, CASE WHEN account_type IN ('CHECKING', 'SAVINGS') THEN 'Priority' ELSE 'Other' END AS account_type_group
FROM accounts;
