WITH store_summary AS (
    SELECT store_id, COUNT(*) AS num_orders
    FROM orders
    WHERE channel NOT IN ('mobile', 'web', 'partner')
    GROUP BY store_id
)
SELECT
    s.id,
    s.city,
    s.opened_date,
    ss.num_orders
FROM stores s
INNER JOIN store_summary ss ON s.id = ss.store_id
ORDER BY city ASC;
