SELECT *
FROM (
    SELECT store_id, SUM(units) total_units
    FROM store_sales
    GROUP BY store_id
) s
WHERE total_units < 80;
