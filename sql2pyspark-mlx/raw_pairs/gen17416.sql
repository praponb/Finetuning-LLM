WITH RecentOrders AS (
    SELECT *, FLOOR(amount) AS amount_whole
    FROM orders
    WHERE channel = 'partner' AND status = 'RETURNED'
),
summary AS (
    SELECT channel, MIN(amount) AS min_amount, ROUND((100 * SUM(CASE WHEN channel = 'mobile' THEN 1 ELSE 0 END)) / COUNT(*), 1) AS pct_mobile
    FROM RecentOrders
    GROUP BY channel
)
SELECT channel, min_amount, pct_mobile
FROM summary;
