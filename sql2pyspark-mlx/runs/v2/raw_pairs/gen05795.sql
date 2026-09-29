SELECT
    DATE_FORMAT(opened_date, 'yyyy-MM') AS opened_month,
    MAX(balance) - MIN(balance) AS balance_range,
    SUM(CASE WHEN account_type = 'CREDIT' THEN 1 ELSE 0 END) AS credit_count,
    AVG(balance) AS avg_balance
FROM accounts
WHERE NOT (account_type IN ('SAVINGS', 'CHECKING') AND status IN ('OPEN', 'FROZEN'))
GROUP BY DATE_FORMAT(opened_date, 'yyyy-MM');
