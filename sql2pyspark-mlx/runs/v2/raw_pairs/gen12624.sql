SELECT
    id,
    store_id,
    sale_date,
    revenue,
    ROUND(AVG(revenue) OVER (PARTITION BY store_id ORDER BY sale_date, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW), 2) AS revenue_moving_avg_3
FROM store_sales
WHERE store_id IN (2, 3)
