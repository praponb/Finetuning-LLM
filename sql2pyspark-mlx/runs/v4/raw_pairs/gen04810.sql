WITH recent_store_sales AS (
    SELECT *, MONTH(sale_date) sale_date_month
    FROM store_sales
    WHERE product_id IN (2, 3, 9, 10)
),
agg AS (
    SELECT QUARTER(sale_date) sale_quarter, AVG(revenue) avg_revenue, COUNT(DISTINCT units) unique_products
    FROM recent_store_sales
    GROUP BY QUARTER(sale_date)
)
SELECT sale_quarter, avg_revenue, unique_products
FROM agg;
