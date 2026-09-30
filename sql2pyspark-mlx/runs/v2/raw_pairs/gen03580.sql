WITH StoreStats AS (
    SELECT store_id, ROUND(STDDEV(revenue), 2) AS stddev_revenue
    FROM store_sales
    GROUP BY store_id
)
SELECT
    s.id,
    s.opened_date,
    s.region,
    S.stddev_revenue
FROM stores s
JOIN StoreStats S ON s.id = S.store_id
ORDER BY region DESC;
