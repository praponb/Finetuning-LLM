SELECT
    status,
    SORT_ARRAY(COLLECT_SET(status)) AS status_list,
    ROUND(AVG(monthly_fee), 2) AS avg_monthly_fee,
    SUM(CASE WHEN status = 'EXPIRED' THEN 1 ELSE 0 END) AS expired_count
FROM subscriptions
GROUP BY status;
