SELECT *
FROM (
    SELECT YEAR(order_date) AS order_year, AVG(discount) AS avg_discount
    FROM orders
    WHERE store_id IN (5, 6, 40)
    GROUP BY YEAR(order_date)
) AS t
WHERE avg_discount >= 0.3
ORDER BY avg_discount DESC, order_year
LIMIT 5
