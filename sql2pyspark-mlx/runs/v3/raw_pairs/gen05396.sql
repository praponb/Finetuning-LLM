WITH valid_orders AS (
    SELECT *
    FROM orders
    WHERE NOT (customer_id IS NULL AND channel = 'mobile' AND shipping_fee > 23)
),
orders_stats AS (
    SELECT QUARTER(order_date) AS order_quarter, status, SORT_ARRAY(COLLECT_SET(channel)) AS channel_list
    FROM valid_orders
    GROUP BY QUARTER(order_date), status
)
SELECT order_quarter, status, channel_list
FROM orders_stats
ORDER BY status;
