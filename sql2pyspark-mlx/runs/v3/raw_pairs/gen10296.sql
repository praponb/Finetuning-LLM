SELECT CASE WHEN account_type IN ('CHECKING', 'LOAN') THEN 'priority' ELSE 'other' END AS account_type_group, ROUND(STDDEV(balance), 2) AS stddev_balance, MAX(balance) AS max_balance
FROM accounts
GROUP BY account_type_group;
