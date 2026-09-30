SELECT
    account_type,
    COUNT(*) cnt,
    SUM(CASE WHEN status = 'OPEN' THEN 1 ELSE 0 END) open_count,
    COUNT(CASE WHEN status = 'CLOSED' THEN 1 END) closed_count
FROM accounts
GROUP BY account_type
ORDER BY account_type DESC;
