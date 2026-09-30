WITH customer_stats AS (
    SELECT customer_id, SORT_ARRAY(COLLECT_SET(channel)) AS channel_list, SUM(CASE WHEN channel = 'web' THEN discount ELSE 0 END) AS web_discount
    FROM orders
    GROUP BY customer_id
)
SELECT customers.signup_date, customer_stats.channel_list, customer_stats.web_discount
FROM customers
LEFT JOIN customer_stats ON customers.id = customer_stats.customer_id
ORDER BY signup_date;
