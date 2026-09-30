WITH base AS (
    SELECT *
    FROM orders
    WHERE store_id IS NULL
),
order_year_summary AS (
    SELECT YEAR(order_date) AS order_year, COUNT(CASE WHEN status = 'COMPLETED' THEN 1 END) AS completed_count
    FROM base
    GROUP BY YEAR(order_date)
)
SELECT order_year, completed_count
FROM order_year_summary
WHERE completed_count >= 4
ORDER BY order_year
