WITH recent_store_sales AS (
    SELECT *
    FROM store_sales
    WHERE product_id IN (6, 10) AND store_id IN (5, 6)
),
Agg AS (
    SELECT DATE_FORMAT(sale_date, 'yyyy-MM') AS sale_month, MIN(sale_date) AS first_sale_date
    FROM recent_store_sales
    GROUP BY DATE_FORMAT(sale_date, 'yyyy-MM')
)
SELECT sale_month, first_sale_date
FROM Agg
