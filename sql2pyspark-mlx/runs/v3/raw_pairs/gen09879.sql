SELECT
    t.is_flagged,
    t.account_id,
    t.merchant,
    a.account_type
FROM transactions t
INNER JOIN accounts a ON a.id = t.account_id
WHERE a.status IN ('CLOSED', 'FROZEN') AND t.merchant NOT IN ('Amazon', 'Starbucks')
ORDER BY a.account_type ASC, t.merchant DESC;
