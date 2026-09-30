WITH src AS (
    SELECT *, DATE_TRUNC('week', sale_date) AS sale_date_period
    FROM store_sales
    WHERE units <= 17 AND sale_date BETWEEN '2023-05-08' AND '2023-05-16'
),
sale_period_month_summary AS (
    SELECT DATE_TRUNC('MONTH', sale_date_period) AS sale_period_month, ROUND(STDDEV(revenue), 2) AS stddev_revenue, MAX(revenue) AS max_revenue
    FROM src
    GROUP BY DATE_TRUNC('MONTH', sale_date_period)
)
SELECT sale_period_month, max_revenue
FROM sale_period_month_summary
ORDER BY max_revenue
