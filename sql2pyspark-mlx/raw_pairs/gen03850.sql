SELECT
    store_id,
    SUM(revenue) AS total_revenue,
    COUNT(*) AS cnt,
    RANK() OVER (ORDER BY SUM(revenue) DESC) AS revenue_rank
FROM store_sales
WHERE revenue BETWEEN 3200 AND 3700
GROUP BY store_id
ORDER BY cnt DESC, store_id
