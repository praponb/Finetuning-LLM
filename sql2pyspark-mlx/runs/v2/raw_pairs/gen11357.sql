SELECT store_id, units_range
FROM (
    SELECT store_id, MAX(units) - MIN(units) AS units_range
    FROM store_sales
    WHERE YEAR(sale_date) >= 2025
    GROUP BY store_id
) AS agg
WHERE units_range > 48
ORDER BY store_id DESC, 2 DESC;
