SELECT
    QUARTER(end_date) AS end_quarter,
    MIN(monthly_fee) AS min_monthly_fee,
    SUM(CASE WHEN status = 'PAUSED' THEN 1 ELSE 0 END) AS paused_count,
    CONCAT_WS(', ', SORT_ARRAY(COLLECT_SET(status))) AS status_values
FROM subscriptions
GROUP BY QUARTER(end_date);
