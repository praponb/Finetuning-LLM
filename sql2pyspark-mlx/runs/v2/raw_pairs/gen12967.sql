SELECT store_id, stddev_units, min_revenue
FROM (
    SELECT store_id, ROUND(STDDEV(units), 2) stddev_units, MIN(revenue) min_revenue
    FROM store_sales
    GROUP BY store_id
) agg
WHERE min_revenue > 2200
ORDER BY store_id DESC
