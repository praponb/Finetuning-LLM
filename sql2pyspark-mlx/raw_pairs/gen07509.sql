WITH orders_by_customer AS (
    SELECT customer_id, ROUND(AVG(discount), 2) AS avg_discount, CONCAT_WS(', ', SORT_ARRAY(COLLECT_SET(channel))) AS channel_values
    FROM orders
    WHERE store_id IS NULL
    GROUP BY customer_id
)
SELECT
    c.age,
    c.segment,
    obc.avg_discount,
    obc.channel_values
FROM customers c
LEFT JOIN orders_by_customer obc ON c.id = obc.customer_id
ORDER BY c.segment;
