SELECT
    account_type,
    ROUND((100 * SUM(CASE WHEN account_type = 'SAVINGS' THEN 1 ELSE 0 END)) / COUNT(*), 1) AS pct_savings,
    MIN(opened_date) AS first_opened_date,
    SUM(CASE WHEN account_type = 'CREDIT' THEN 1 ELSE 0 END) AS credit_count
FROM accounts
WHERE balance <= 72000 AND account_type NOT IN ('CREDIT', 'CHECKING') AND opened_date < '2024-06-01'
GROUP BY account_type
ORDER BY pct_savings DESC, credit_count ASC;
