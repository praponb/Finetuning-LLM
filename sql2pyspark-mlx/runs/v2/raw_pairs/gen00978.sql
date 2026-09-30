SELECT
    id,
    store_id,
    sale_date,
    revenue,
    revenue - LAG(revenue, 2) OVER (PARTITION BY store_id ORDER BY sale_date, id) AS revenue_change
FROM store_sales;
