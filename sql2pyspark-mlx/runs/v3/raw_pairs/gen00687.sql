SELECT *
FROM (
    SELECT status, CONCAT_WS(', ', SORT_ARRAY(COLLECT_SET(status))) AS status_values, AVG(balance) AS avg_balance
    FROM accounts
    GROUP BY status
) AS t
WHERE avg_balance >= 57000
ORDER BY avg_balance DESC, status_values;
