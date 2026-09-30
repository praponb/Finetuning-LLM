WITH filtered_orders AS (
    SELECT *
    FROM orders
    WHERE store_id IN (34, 39) AND status = 'SHIPPED'
),
channel_summary AS (
    SELECT channel, MAX(discount) AS max_discount
    FROM filtered_orders
    GROUP BY channel
)
SELECT *
FROM channel_summary;
