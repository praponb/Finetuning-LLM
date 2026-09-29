WITH base AS (
    SELECT *
    FROM store_sales
    WHERE (product_id IN (2, 9, 10) AND store_id = 6) OR sale_date > '2024-03-01'
),
summary AS (
    SELECT store_id, QUARTER(sale_date) AS sale_quarter, ROUND(AVG(revenue), 2) AS avg_revenue
    FROM base
    GROUP BY store_id, QUARTER(sale_date)
)
SELECT *
FROM summary
WHERE avg_revenue < 22
ORDER BY store_id DESC, avg_revenue DESC;
