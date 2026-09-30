SELECT
    DATE_TRUNC('WEEK', paid_at) AS week_start,
    CASE WHEN amount >= 1670 THEN 'HIGH' ELSE 'LOW' END AS amount_tier,
    ROUND(AVG(amount), 2) AS avg_amount,
    SORT_ARRAY(COLLECT_SET(status)) AS status_list,
    ROUND(SUM(amount) / NULLIF(COUNT(CASE WHEN status = 'FAILED' THEN 1 END), 0), 2) AS amount_per_failed,
    COUNT(*) AS row_count,
    STDDEV_SAMP(amount) AS amount_stddev,
    GROUPING_ID() AS grouping_id
FROM payments
GROUP BY CUBE (DATE_TRUNC('WEEK', paid_at), CASE WHEN amount >= 1670 THEN 'HIGH' ELSE 'LOW' END)
ORDER BY week_start DESC NULLS LAST;
