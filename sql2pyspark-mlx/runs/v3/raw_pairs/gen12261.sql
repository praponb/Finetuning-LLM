SELECT *
FROM (
    SELECT product_id, MAX(units) - MIN(units) AS units_range
    FROM store_sales
    GROUP BY product_id
) AS t
WHERE units_range >= 66
ORDER BY product_id DESC
LIMIT 3
