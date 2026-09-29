WITH latest_match AS (
    SELECT s.*, latest.amount
    FROM stores s
    LEFT JOIN LATERAL (
            SELECT c.amount
            FROM orders c
            WHERE c.store_id = s.id
            ORDER BY c.shipping_fee DESC, c.id DESC
            LIMIT 1
        ) latest ON TRUE
)
SELECT
    region,
    COUNT(*) FILTER (WHERE region = 'South') AS south_rows,
    SUM(CASE WHEN region = 'South' THEN amount ELSE 0 END) AS south_amount,
    SIZE(COLLECT_SET(city)) AS num_cities
FROM latest_match
GROUP BY 1
ORDER BY region;
