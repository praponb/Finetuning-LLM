SELECT
    QUARTER(order_date) order_quarter,
    SORT_ARRAY(COLLECT_SET(status)) status_list,
    SORT_ARRAY(COLLECT_SET(channel)) channel_list,
    ROUND(AVG(discount), 2) avg_discount
FROM orders
GROUP BY QUARTER(order_date)
ORDER BY avg_discount ASC;
