SELECT
    id,
    sale_date,
    revenue,
    product_id,
    store_id,
    units,
    ROUND(AVG(revenue) OVER (ORDER BY sale_date, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW), 2) AS revenue_moving_avg_7,
    RANK() OVER (PARTITION BY product_id, store_id ORDER BY units DESC) AS units_rank
FROM store_sales
WHERE product_id IN (5, 6, 9)
