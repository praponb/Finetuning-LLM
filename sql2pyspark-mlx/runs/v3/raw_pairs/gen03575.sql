SELECT store_id, CASE WHEN YEAR(order_date) < 2024 THEN 'before_2024' WHEN YEAR(order_date) = 2024 THEN '2024' ELSE 'after_2024' END AS order_cohort, CONCAT_WS(', ', SORT_ARRAY(COLLECT_SET(channel))) AS channel_values
FROM orders
GROUP BY store_id, order_cohort
