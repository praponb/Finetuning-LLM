SELECT
    id,
    product_id,
    revenue,
    ROW_NUMBER() OVER (PARTITION BY product_id ORDER BY revenue ASC, id ASC) AS row_seq,
    ROUND(units / SUM(units) OVER (PARTITION BY product_id, store_id), 4) AS units_share,
    CORR(revenue, units) OVER (PARTITION BY store_id, product_id) AS corr_revenue_units,
    SUM(units) OVER w_running AS sale_date_period_sum_units
FROM store_sales
WHERE units IS NULL
WINDOW w_running AS (PARTITION BY DATE_TRUNC('MONTH', sale_date), store_id);
