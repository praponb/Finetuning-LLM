SELECT DATE_FORMAT(start_date, 'yyyy-MM') AS start_month, COUNT(*) AS num_subscriptions, SORT_ARRAY(COLLECT_SET(status)) AS status_list
FROM subscriptions
GROUP BY 1
HAVING COUNT(*) >= 3
ORDER BY num_subscriptions DESC;
