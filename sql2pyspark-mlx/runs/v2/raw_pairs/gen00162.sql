WITH orders_by_store AS (
    SELECT store_id, ROUND(STDDEV(discount), 2) AS stddev_discount, ROUND((100 * SUM(CASE WHEN channel = 'store' THEN 1 ELSE 0 END)) / COUNT(*), 1) AS pct_store
    FROM orders
    WHERE shipping_fee >= 17
    GROUP BY store_id
)
SELECT
    s.region,
    s.city,
    obs.stddev_discount,
    obs.pct_store
FROM stores s
INNER JOIN orders_by_store obs ON s.id = obs.store_id
ORDER BY stddev_discount DESC, s.city
