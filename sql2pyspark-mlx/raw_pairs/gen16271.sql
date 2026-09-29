SELECT status, order_month, completed_count
FROM (
    SELECT status, DATE_FORMAT(order_date, 'yyyy-MM') AS order_month, COUNT(CASE WHEN status = 'COMPLETED' THEN 1 END) AS completed_count
    FROM orders
    GROUP BY status, DATE_FORMAT(order_date, 'yyyy-MM')
) AS s
WHERE completed_count > 4
ORDER BY status DESC, order_month
LIMIT 5
