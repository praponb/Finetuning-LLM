SELECT
    id,
    store_id,
    sale_date,
    revenue,
    product_id,
    units,
    ROUND(AVG(revenue) OVER (PARTITION BY store_id ORDER BY sale_date, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW), 2) revenue_moving_avg_3,
    ROW_NUMBER() OVER (PARTITION BY product_id ORDER BY units, id) row_num
FROM store_sales
ORDER BY store_id DESC;
