WITH filtered_accounts AS (
    SELECT *
    FROM accounts
    WHERE balance > 76000
),
accounts_stats AS (
    SELECT account_type, COUNT(DISTINCT balance) AS unique_statuss, CONCAT_WS(', ', SORT_ARRAY(COLLECT_SET(account_type))) AS account_type_values
    FROM filtered_accounts
    GROUP BY account_type
)
SELECT account_type, account_type_values
FROM accounts_stats;
