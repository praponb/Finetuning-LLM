WITH store_summary AS (
    SELECT store_id, ROUND(STDDEV(units), 2) AS stddev_units, ROUND(STDDEV(revenue), 2) AS stddev_revenue
    FROM store_sales
    GROUP BY store_id
)
SELECT
    s.id,
    s.sqft,
    ss.stddev_units,
    ss.stddev_revenue
FROM stores s
INNER JOIN store_summary ss ON s.id = ss.store_id;
