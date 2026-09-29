SELECT
    DATE_TRUNC('MONTH', opened_date) AS opened_month,
    status,
    MIN(opened_date) AS min_opened_date,
    SORT_ARRAY(COLLECT_SET(account_type)) AS account_type_list,
    MAX(opened_date) AS last_opened_date
FROM accounts
GROUP BY opened_month, status;
