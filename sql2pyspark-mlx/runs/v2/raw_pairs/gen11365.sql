WITH src AS (
    SELECT *
    FROM store_sales
    WHERE units < 33
),
store_sales_stats AS (
    SELECT store_id, DATE_TRUNC('MONTH', sale_date) AS sale_month, MAX(sale_date) AS latest_sale_date
    FROM src
    GROUP BY store_id, DATE_TRUNC('MONTH', sale_date)
)
SELECT store_id, latest_sale_date
FROM store_sales_stats
ORDER BY store_id
