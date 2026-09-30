SELECT
    id,
    sale_date,
    store_id,
    revenue,
    units,
    AVG(revenue) OVER (PARTITION BY DATE_TRUNC('week', sale_date), store_id) AS sale_date_period_avg_revenue,
    ROUND(AVG(units) OVER (PARTITION BY store_id ORDER BY sale_date, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW), 2) AS units_moving_avg_7
FROM store_sales
WHERE store_id = 1
