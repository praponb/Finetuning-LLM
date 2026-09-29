SELECT status, row_count, account_type_values
FROM (
    SELECT status, COUNT(*) AS row_count, CONCAT_WS(', ', SORT_ARRAY(COLLECT_SET(account_type))) AS account_type_values
    FROM accounts
    WHERE status = 'CLOSED'
    GROUP BY status
) AS agg
WHERE row_count < 4
