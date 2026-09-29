WITH valid AS (
    SELECT
        *,
        ROUND(discount / amount, 2) AS discount_per_amount,
        COALESCE(channel, 'UNKNOWN') AS resolved_channel
    FROM orders
    WHERE customer_id IS NOT NULL
      AND channel != 'store'
)
SELECT
    d.city,
    d.region,
    COUNT(*) AS num_rows,
    MAX(e.discount_per_amount) AS max_discount_per_amount
FROM valid e
LEFT JOIN stores d ON e.store_id = d.id
GROUP BY d.city, d.region
ORDER BY max_discount_per_amount
