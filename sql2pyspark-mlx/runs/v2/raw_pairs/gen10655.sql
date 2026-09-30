WITH src AS (
    SELECT *, units * revenue AS revenue_per_unit
    FROM store_sales
    WHERE product_id = 10
),
sale_month_summary AS (
    SELECT DATE_TRUNC('MONTH', sale_date) AS sale_month, MAX(revenue_per_unit) - MIN(revenue_per_unit) AS revenue_per_unit_range
    FROM src
    GROUP BY DATE_TRUNC('MONTH', sale_date)
)
SELECT sale_month, revenue_per_unit_range
FROM sale_month_summary
WHERE revenue_per_unit_range < 38;
