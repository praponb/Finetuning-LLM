WITH FilteredStoreSales AS (
    SELECT *, DATE_SUB(sale_date, 26) sale_date_minus_26d
    FROM store_sales
    WHERE revenue < 3100 OR product_id IN (1, 4, 8, 10)
),
store_sales_stats AS (
    SELECT
        product_id,
        store_id,
        SUM(revenue) total_revenue,
        MIN(sale_date_minus_26d) first_sale_date_minus_26d
    FROM FilteredStoreSales
    GROUP BY product_id, store_id
)
SELECT *
FROM store_sales_stats
ORDER BY first_sale_date_minus_26d ASC;
