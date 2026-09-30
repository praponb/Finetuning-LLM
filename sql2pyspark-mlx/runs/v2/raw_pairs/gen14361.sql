WITH orders_by_store AS (
    SELECT store_id, COUNT(CASE WHEN channel = 'mobile' THEN 1 END) AS mobile_count, ROUND((100 * SUM(CASE WHEN status = 'SHIPPED' THEN 1 ELSE 0 END)) / COUNT(*), 1) AS pct_shipped
    FROM orders
    GROUP BY store_id
)
SELECT
    s.sqft,
    s.city,
    obs.mobile_count,
    obs.pct_shipped
FROM stores s
INNER JOIN orders_by_store obs ON s.id = obs.store_id
WHERE obs.mobile_count >= 6
ORDER BY pct_shipped DESC;
