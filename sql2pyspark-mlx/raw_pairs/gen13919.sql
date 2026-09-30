WITH src AS (
    SELECT *
    FROM orders
    WHERE order_date BETWEEN '2023-11-14' AND '2025-08-01' AND amount < 650
),
order_month_summary AS (
    SELECT DATE_TRUNC('MONTH', order_date) AS order_month, COUNT(*) AS order_count, COUNT(DISTINCT amount) AS unique_customers
    FROM src
    GROUP BY DATE_TRUNC('MONTH', order_date)
)
SELECT order_month, unique_customers
FROM order_month_summary
