SELECT YEAR(end_date) AS end_year, DATE_TRUNC('MONTH', start_date) AS start_month, SORT_ARRAY(COLLECT_SET(status)) AS status_list
FROM subscriptions
GROUP BY YEAR(end_date), DATE_TRUNC('MONTH', start_date)
ORDER BY end_year DESC, start_month
LIMIT 3;
