WITH cleaned AS (
    SELECT *, cost * price AS margin
    FROM products
    WHERE launch_date > TO_DATE('2023-07-13')
)
SELECT
    id,
    product_name,
    price,
    launch_date,
    tags,
    supplier_id,
    AVG(cost) OVER (PARTITION BY category, brand) AS category_avg_cost,
    AVG(cost) OVER (PARTITION BY supplier_id ORDER BY margin, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS cost_avg_3
FROM cleaned
