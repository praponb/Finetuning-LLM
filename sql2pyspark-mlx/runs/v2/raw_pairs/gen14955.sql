SELECT c.city, SUM(s.monthly_fee) AS total_monthly_fee, CONCAT_WS(', ', SORT_ARRAY(COLLECT_SET(s.status))) AS status_values
FROM subscriptions s
INNER JOIN customers c ON c.id = s.customer_id
GROUP BY c.city
HAVING SUM(s.monthly_fee) >= 740
ORDER BY c.city DESC, status_values DESC
