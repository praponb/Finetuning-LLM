SELECT
    opened_month,
    account_type,
    closed_count,
    account_type_values
FROM (
    SELECT
        DATE_TRUNC('MONTH', opened_date) AS opened_month,
        account_type,
        SUM(CASE WHEN status = 'CLOSED' THEN 1 ELSE 0 END) AS closed_count,
        CONCAT_WS(', ', SORT_ARRAY(COLLECT_SET(account_type))) AS account_type_values
    FROM accounts
    WHERE account_type IN ('CHECKING', 'LOAN')
    GROUP BY DATE_TRUNC('MONTH', opened_date), account_type
) AS t
WHERE closed_count > 72
ORDER BY closed_count DESC, opened_month, account_type
LIMIT 5;
