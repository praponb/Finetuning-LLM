SELECT
    id,
    store_id,
    product_id,
    units,
    revenue,
    LAG(units, 1) OVER w_running AS prev_units,
    NTILE(10) OVER w_recent AS units_bucket,
    SUM(units) OVER (PARTITION BY YEAR(sale_date), product_id ORDER BY sale_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_units,
    LAG(revenue, 1) OVER w_hist AS prev_revenue
FROM store_sales
WHERE store_id IN (1, 3, 4)
WINDOW w_running AS (PARTITION BY product_id ORDER BY sale_date, id), w_recent AS (PARTITION BY product_id ORDER BY units, id), w_hist AS (PARTITION BY DATE_TRUNC('week', sale_date), store_id ORDER BY units, id)
