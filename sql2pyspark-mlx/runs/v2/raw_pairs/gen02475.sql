WITH src AS (
    SELECT *, CAST(price AS INT) AS price_int
    FROM products
    WHERE ARRAY_CONTAINS(tags, 'clearance') AND brand = 'Globex'
),
summary AS (
    SELECT DATE_FORMAT(launch_date, 'yyyy-MM') AS launch_month, SUM(stock_qty) AS total_stock_qty
    FROM src
    GROUP BY DATE_FORMAT(launch_date, 'yyyy-MM')
)
SELECT launch_month, total_stock_qty
FROM summary
