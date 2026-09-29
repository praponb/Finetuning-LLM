SELECT
    DATE_TRUNC('YEAR', order_date) AS year_start,
    CASE WHEN amount >= 1220 THEN 'HIGH' ELSE 'LOW' END AS amount_tier,
    SUM(CASE WHEN channel = 'web' THEN amount ELSE 0 END) AS web_amount,
    COUNT(*) AS cnt,
    SORT_ARRAY(COLLECT_SET(status)) AS status_list,
    SORT_ARRAY(COLLECT_SET(channel)) AS channel_list
FROM orders
GROUP BY ROLLUP (DATE_TRUNC('YEAR', order_date), CASE WHEN amount >= 1220 THEN 'HIGH' ELSE 'LOW' END)
ORDER BY web_amount DESC NULLS LAST;
