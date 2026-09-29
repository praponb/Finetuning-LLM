WITH recent_store_sales AS (
    SELECT *
    FROM store_sales
    WHERE MONTH(sale_date) IN (2, 5, 12)
),
product_id_summary AS (
    SELECT product_id, SUM(units) AS total_units, COUNT(units) AS units_count
    FROM recent_store_sales
    GROUP BY product_id
)
SELECT *
FROM product_id_summary
WHERE units_count >= 4;
