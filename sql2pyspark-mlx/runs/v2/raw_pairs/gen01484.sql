SELECT
    id,
    product_id,
    sale_date,
    units,
    ROUND(AVG(units) OVER (PARTITION BY product_id ORDER BY sale_date, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW), 2) AS units_moving_avg_3
FROM store_sales
WHERE sale_date <= '2023-10-01'
