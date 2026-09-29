WITH store_stats AS (
    SELECT store_id, SUM(CASE WHEN channel = 'store' THEN amount ELSE 0 END) AS store_amount, COUNT(amount) AS amount_count
    FROM orders
    WHERE channel IN ('web', 'partner')
    GROUP BY store_id
)
SELECT
    s.id,
    s.region,
    ss.store_amount,
    ss.amount_count
FROM stores s
LEFT JOIN store_stats ss ON s.id = ss.store_id
WHERE ss.amount_count >= 6;
