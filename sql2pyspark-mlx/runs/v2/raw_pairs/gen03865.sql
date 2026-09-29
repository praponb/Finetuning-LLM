SELECT account_type, balance_bucket, unique_statuss
FROM (
    SELECT account_type, CASE WHEN balance >= 78000 THEN 'high' WHEN balance >= 15000 THEN 'mid' ELSE 'low' END AS balance_bucket, COUNT(DISTINCT balance) AS unique_statuss
    FROM accounts
    WHERE opened_date <= '2024-06-01'
    GROUP BY account_type, CASE WHEN balance >= 78000 THEN 'high' WHEN balance >= 15000 THEN 'mid' ELSE 'low' END
) AS agg
WHERE unique_statuss < 9
ORDER BY balance_bucket DESC;
