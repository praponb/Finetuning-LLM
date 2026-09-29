WITH FilteredOrders AS (
    SELECT *, CASE WHEN status IN ('COMPLETED', 'PENDING') THEN 'Tier 1' ELSE 'Other' END AS status_group
    FROM orders
    WHERE discount <= 0.35 OR store_id IN (24, 39)
),
summary AS (
    SELECT channel, COUNT(*) AS total_count
    FROM FilteredOrders
    GROUP BY channel
)
SELECT channel, total_count
FROM summary
WHERE total_count < 8
ORDER BY total_count;
