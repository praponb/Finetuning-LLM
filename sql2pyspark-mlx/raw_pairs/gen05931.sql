SELECT
    id,
    store_id,
    revenue,
    product_id,
    sale_date,
    ROW_NUMBER() OVER (PARTITION BY store_id ORDER BY revenue DESC, id) AS row_num,
    ROUND(AVG(revenue) OVER (PARTITION BY product_id, store_id ORDER BY sale_date, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW), 2) AS revenue_moving_avg_7
FROM store_sales;
