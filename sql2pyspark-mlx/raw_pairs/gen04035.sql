WITH valid AS (
    SELECT
        *,
        ADD_MONTHS(opened_date, 1) opened_date_plus_months,
        COALESCE(status, 'UNKNOWN') resolved_status
    FROM accounts
    WHERE status IS NOT NULL
      AND status IS DISTINCT FROM 'CLOSED'
)
SELECT
    DATE_TRUNC('WEEK', opened_date_plus_months) AS week_start,
    DATE_TRUNC('QUARTER', opened_date) AS quarter_start,
    SORT_ARRAY(COLLECT_SET(account_type)) AS account_type_list,
    STDDEV_SAMP(balance) AS balance_stddev,
    COUNT_IF(account_type = 'CREDIT') AS credit_events,
    COUNT(CASE WHEN account_type = 'SAVINGS' THEN 1 END) AS savings_count
FROM valid
GROUP BY week_start, quarter_start;
