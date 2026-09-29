WITH base AS (
    SELECT
        *,
        CASE WHEN units >= 45 THEN 'High' WHEN units >= 15 THEN 'Medium' ELSE 'Low' END AS units_tier,
        LEAST(units, revenue) AS least_units_revenue
    FROM store_sales
    WHERE sale_date > DATE '2023-03-19'
)
SELECT
    *,
    AVG(units) OVER (PARTITION BY store_id ORDER BY sale_date ASC, id ASC ROWS BETWEEN 5 PRECEDING AND CURRENT ROW) AS units_avg_5,
    ROW_NUMBER() OVER w_hist AS seq_no,
    SUM(revenue) OVER (PARTITION BY product_id, store_id ORDER BY revenue ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_revenue
FROM base
WINDOW w_hist AS (PARTITION BY product_id ORDER BY revenue ASC, id ASC);
