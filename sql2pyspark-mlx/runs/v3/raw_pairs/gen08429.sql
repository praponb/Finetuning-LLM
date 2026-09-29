SELECT
    opened_date,
    balance,
    account_type,
    CASE WHEN account_type IN ('CHECKING', 'LOAN') THEN 'Group A' ELSE 'Other' END AS account_type_group,
    CASE WHEN balance >= 66000 THEN 'High' WHEN balance >= 30000 THEN 'Medium' ELSE 'Low' END AS balance_level
FROM accounts
WHERE account_type = 'LOAN' AND opened_date <= '2024-02-01';
