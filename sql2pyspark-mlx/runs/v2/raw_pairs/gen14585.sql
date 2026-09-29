SELECT *
FROM (
    SELECT store_id, MIN(revenue) AS min_revenue, MAX(units) - MIN(units) AS units_range
    FROM store_sales
    GROUP BY store_id
) AS agg
WHERE min_revenue < 1600
