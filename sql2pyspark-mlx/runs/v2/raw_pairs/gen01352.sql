SELECT
    account_type,
    CONCAT_WS(', ', SORT_ARRAY(COLLECT_SET(status))) status_values,
    MAX(opened_date) last_opened_date,
    SUM(CASE WHEN status = 'FROZEN' THEN 1 ELSE 0 END) frozen_count
FROM accounts
WHERE opened_date < '2023-07-01'
GROUP BY account_type
