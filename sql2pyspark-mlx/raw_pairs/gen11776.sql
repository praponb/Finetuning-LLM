SELECT
    id,
    sale_date,
    product_id,
    revenue,
    AVG(revenue) OVER (PARTITION BY MONTH(sale_date), product_id) AS sale_date_month_avg_revenue
FROM store_sales
