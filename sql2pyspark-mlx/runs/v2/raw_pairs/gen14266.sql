SELECT *
FROM (
    SELECT DATE_FORMAT(order_date, 'yyyy-MM') order_month, SUM(CASE WHEN channel = 'partner' THEN 1 ELSE 0 END) partner_count
    FROM orders
    GROUP BY DATE_FORMAT(order_date, 'yyyy-MM')
) t
WHERE partner_count >= 74;
