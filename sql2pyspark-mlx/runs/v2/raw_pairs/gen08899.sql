WITH store_summary AS (
    SELECT store_id, ROUND((100 * SUM(CASE WHEN channel = 'partner' THEN 1 ELSE 0 END)) / COUNT(*), 1) AS pct_partner
    FROM orders
    WHERE status = 'COMPLETED'
    GROUP BY store_id
)
SELECT s.opened_date, s.sqft, ss.pct_partner
FROM stores s
INNER JOIN store_summary ss ON s.id = ss.store_id
ORDER BY s.sqft DESC;
