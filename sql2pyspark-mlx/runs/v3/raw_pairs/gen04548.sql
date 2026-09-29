SELECT
    CASE WHEN status IN ('CANCELLED', 'RETURNED') THEN 'tier_1' ELSE 'other' END AS status_group,
    DATE_FORMAT(order_date, 'yyyy-MM') AS order_month,
    CONCAT_WS(', ', SORT_ARRAY(COLLECT_SET(channel))) AS channel_values,
    SORT_ARRAY(COLLECT_SET(channel)) AS channel_list
FROM orders
WHERE order_date <= '2025-02-08' AND amount BETWEEN 1100 AND 1160
GROUP BY 1, 2
ORDER BY order_month;
