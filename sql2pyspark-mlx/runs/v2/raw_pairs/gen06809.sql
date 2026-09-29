SELECT store_id, avg_units, stddev_revenue
FROM (
    SELECT store_id, ROUND(AVG(units), 2) avg_units, ROUND(STDDEV(revenue), 2) stddev_revenue
    FROM store_sales
    GROUP BY store_id
) t
WHERE avg_units < 26
ORDER BY store_id;
