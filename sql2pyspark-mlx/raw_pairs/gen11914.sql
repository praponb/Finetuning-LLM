SELECT CASE WHEN status IN ('FROZEN', 'CLOSED') THEN 'tier_1' ELSE 'other' END AS status_group, CONCAT_WS(', ', SORT_ARRAY(COLLECT_SET(account_type))) AS account_type_values, COUNT(DISTINCT balance) AS unique_account_types
FROM accounts
WHERE account_type IN ('CHECKING', 'SAVINGS', 'LOAN')
GROUP BY 1
HAVING MAX(balance) > 49000
ORDER BY status_group, account_type_values
