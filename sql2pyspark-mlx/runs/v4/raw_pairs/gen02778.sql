WITH enriched AS (
    SELECT
        s.*,
        p.launch_date,
        p.cost,
        p.stock_qty
    FROM store_sales s
    INNER JOIN products p
        ON s.product_id = p.id
),
latest AS (
    SELECT *
    FROM enriched
    QUALIFY ROW_NUMBER() OVER (PARTITION BY store_id ORDER BY revenue DESC, id) = 1
),
metrics AS (
    SELECT
        id,
        store_id,
        product_id,
        sale_date,
        units,
        revenue,
        launch_date,
        cost,
        stock_qty,
        AVG(units) OVER (PARTITION BY store_id ORDER BY revenue, id ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING) AS units_avg_3_prior,
        SUM(cost) OVER (ORDER BY launch_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_cost,
        ROUND(revenue / SUM(revenue) OVER (PARTITION BY product_id), 4) AS revenue_share,
        ROW_NUMBER() OVER (ORDER BY revenue DESC, id) AS row_seq
    FROM latest
)
SELECT
    CASE WHEN row_seq >= 9 THEN 'HIGH' ELSE 'LOW' END AS row_seq_tier,
    DATE_TRUNC('MONTH', launch_date) AS activity_month,
    PERCENTILE_APPROX(running_cost, 0.9) AS running_cost_p90,
    BIT_AND(units) AS units_bit_and,
    APPROX_PERCENTILE(units, ARRAY(0.25, 0.5, 0.75)) AS units_quartiles,
    MAX(cost) AS max_cost,
    APPROX_PERCENTILE(stock_qty, ARRAY(0.25, 0.5, 0.75)) AS stock_qty_quartiles,
    MIN(revenue_share) AS min_revenue_share
FROM metrics
GROUP BY CASE WHEN row_seq >= 9 THEN 'HIGH' ELSE 'LOW' END, DATE_TRUNC('MONTH', launch_date)
ORDER BY min_revenue_share DESC;
