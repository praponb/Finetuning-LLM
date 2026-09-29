SELECT
    id,
    store_id,
    sale_date,
    revenue,
    LEAD(revenue, 1) OVER (PARTITION BY store_id ORDER BY sale_date, id) AS next_revenue
FROM store_sales
WHERE units >= 13
ORDER BY revenue DESC, 2 DESC;
