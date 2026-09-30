SELECT
    id,
    store_id,
    sale_date,
    revenue,
    SUM(revenue) OVER (PARTITION BY store_id ORDER BY sale_date, id) AS running_revenue
FROM store_sales
ORDER BY running_revenue DESC;
