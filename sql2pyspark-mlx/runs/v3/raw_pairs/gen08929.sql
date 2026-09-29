SELECT
    a.status,
    a.account_type,
    t.merchant,
    CASE WHEN t.merchant = 'Shell' THEN 1 ELSE 0 END AS is_shell
FROM accounts a
INNER JOIN transactions t ON a.id = t.account_id
WHERE t.is_flagged
ORDER BY account_type
