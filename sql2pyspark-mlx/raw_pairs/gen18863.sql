SELECT *
FROM (
    SELECT account_type, COUNT(CASE WHEN status = 'FROZEN' THEN 1 END) AS frozen_count, MAX(balance) - MIN(balance) AS balance_range
    FROM accounts
    WHERE status = 'FROZEN'
    GROUP BY account_type
) AS sub
WHERE balance_range < 73
ORDER BY frozen_count DESC, balance_range DESC;
