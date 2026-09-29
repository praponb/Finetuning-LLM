WITH recent_stores AS (
    SELECT *, LENGTH(store_name) AS store_name_length
    FROM stores
    WHERE store_name LIKE 'S%' AND UPPER(store_name) LIKE '%IAM%'
),
agg AS (
    SELECT region, MAX(sqft) - MIN(sqft) AS sqft_range, ROUND(AVG(store_name_length), 2) AS avg_store_name_length
    FROM recent_stores
    GROUP BY region
)
SELECT region, avg_store_name_length
FROM agg;
