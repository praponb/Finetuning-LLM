SELECT DATE_FORMAT(paid_at, 'yyyy-MM') AS paid_month, CONCAT_WS(', ', SORT_ARRAY(COLLECT_SET(method))) AS method_values, AVG(amount) AS avg_amount
FROM payments
GROUP BY DATE_FORMAT(paid_at, 'yyyy-MM')
HAVING AVG(amount) < 1320
