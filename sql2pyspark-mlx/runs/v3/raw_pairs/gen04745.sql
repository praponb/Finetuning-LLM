WITH orders_by_store AS (
    SELECT store_id, SUM(CASE WHEN status = 'PENDING' THEN 1 ELSE 0 END) AS pending_count
    FROM orders
    GROUP BY store_id
)
SELECT
    s.id,
    s.region,
    s.store_name,
    obs.pending_count
FROM stores s
INNER JOIN orders_by_store obs ON s.id = obs.store_id
WHERE obs.pending_count >= 51;
