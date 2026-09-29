WITH src AS (
    SELECT *, TRIM(status) AS clean_status
    FROM orders
    WHERE channel != 'partner'
),
Agg AS (
    SELECT channel, COUNT(CASE WHEN channel = 'web' THEN 1 END) AS web_count, MAX(shipping_fee) AS max_shipping_fee
    FROM src
    GROUP BY channel
)
SELECT *
FROM Agg
ORDER BY channel, max_shipping_fee DESC
LIMIT 10
