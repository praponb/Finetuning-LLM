SELECT
    id,
    store_id,
    product_id,
    sale_date,
    revenue,
    ROUND(AVG(revenue) OVER (PARTITION BY store_id, product_id ORDER BY sale_date, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW), 2) AS revenue_moving_avg_7,
    ROW_NUMBER() OVER (PARTITION BY product_id ORDER BY revenue, id) AS rn
FROM store_sales
