WITH ValidStoreSales AS (
    SELECT *
    FROM store_sales
    WHERE revenue < 3700
),
store_id_summary AS (
    SELECT store_id, ROUND(STDDEV(units), 2) stddev_units
    FROM ValidStoreSales
    GROUP BY store_id
)
SELECT store_id, stddev_units
FROM store_id_summary
ORDER BY stddev_units, store_id
