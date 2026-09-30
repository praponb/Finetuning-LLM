SELECT customer_id, ROUND(STDDEV(discount), 2) AS stddev_discount, SORT_ARRAY(COLLECT_SET(status)) AS status_list FROM orders GROUP BY customer_id HAVING stddev_discount >= 40 ORDER BY 1 DESC;
