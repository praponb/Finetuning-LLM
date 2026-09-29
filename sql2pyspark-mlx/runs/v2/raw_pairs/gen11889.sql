SELECT
    id,
    store_id,
    revenue,
    PERCENT_RANK() OVER (PARTITION BY store_id ORDER BY revenue) AS revenue_pct_rank
FROM store_sales
