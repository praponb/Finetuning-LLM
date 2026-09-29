SELECT
    id,
    store_id,
    sale_date,
    revenue,
    product_id,
    units,
    SUM(revenue) OVER (PARTITION BY store_id ORDER BY sale_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) running_revenue,
    ROUND(AVG(units) OVER (PARTITION BY product_id ORDER BY sale_date, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW), 2) units_moving_avg_7
FROM store_sales
