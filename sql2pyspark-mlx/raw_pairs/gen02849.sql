SELECT account_type, ROUND((100 * SUM(CASE WHEN account_type = 'CHECKING' THEN 1 ELSE 0 END)) / COUNT(*), 1) AS pct_checking
FROM accounts
WHERE account_type NOT IN ('SAVINGS', 'CHECKING') AND status IS NOT NULL
GROUP BY account_type
HAVING ROUND((100 * SUM(CASE WHEN account_type = 'CHECKING' THEN 1 ELSE 0 END)) / COUNT(*), 1) >= 74;
