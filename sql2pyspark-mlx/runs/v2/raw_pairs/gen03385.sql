SELECT account_type, balance_range
FROM (
    SELECT account_type, MAX(balance) - MIN(balance) AS balance_range
    FROM accounts
    GROUP BY account_type
) AS agg
WHERE balance_range < 76
ORDER BY balance_range DESC, account_type
LIMIT 10;
