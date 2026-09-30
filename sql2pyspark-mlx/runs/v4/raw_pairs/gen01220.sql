SELECT
    DATE_TRUNC('QUARTER', order_date) AS quarter_start,
    store_id,
    DATE_TRUNC('WEEK', order_date) AS week_start,
    PERCENTILE_APPROX(amount, ARRAY(0.25, 0.5, 0.75)) AS amount_quartiles,
    ROUND(SUM(amount) / NULLIF(COUNT(CASE WHEN channel = 'partner' THEN 1 END), 0), 2) AS amount_per_partner,
    COUNT(DISTINCT id) AS distinct_ids,
    GROUPING(DATE_TRUNC('QUARTER', order_date)) AS gid
FROM orders
GROUP BY GROUPING SETS ((DATE_TRUNC('QUARTER', order_date), store_id, DATE_TRUNC('WEEK', order_date)), (DATE_TRUNC('QUARTER', order_date), store_id), (DATE_TRUNC('QUARTER', order_date)))
