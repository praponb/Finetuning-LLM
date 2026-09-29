WITH recent_store_sales AS (
    SELECT *
    FROM store_sales
    WHERE product_id IN (1, 2, 5, 9) AND (store_id IN (1, 2, 3, 5) OR revenue < 2200)
),
summary AS (
    SELECT store_id, MIN(sale_date) AS min_sale_date, MAX(units) - MIN(units) AS units_range
    FROM recent_store_sales
    GROUP BY store_id
)
SELECT store_id, min_sale_date, units_range
FROM summary
WHERE units_range >= 40
ORDER BY store_id ASC, units_range DESC
LIMIT 10;
