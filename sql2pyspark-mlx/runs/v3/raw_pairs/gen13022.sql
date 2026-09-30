SELECT
    id,
    product_id,
    revenue,
    sale_date,
    units,
    AVG(revenue) OVER (PARTITION BY product_id) AS product_id_avg_revenue,
    SUM(units) OVER (PARTITION BY product_id ORDER BY sale_date, id) AS running_units
FROM store_sales
ORDER BY product_id_avg_revenue DESC
