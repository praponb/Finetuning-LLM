WITH valid AS (
    SELECT
        id,
        store_id,
        product_id,
        units,
        revenue,
        LEAST(revenue, units) AS least_revenue_units
    FROM store_sales
    WHERE sale_date >= DATE '2023-02-20'
),
enriched AS (
    SELECT
        src.*,
        b.launch_date,
        b.price
    FROM valid src
    LEFT JOIN products b
        ON src.product_id = b.id
),
joined AS (
    SELECT
        s.*,
        b.stock_qty,
        b.product_name
    FROM enriched s
    LEFT JOIN products b
        ON s.product_id = b.id
),
metrics AS (
    SELECT
        *,
        LEAD(stock_qty) OVER w_recent AS next_stock_qty,
        LEAD(units) OVER w_recent AS next_units,
        LAG(units) OVER w_recent AS prev_units,
        ROW_NUMBER() OVER w_recent AS row_seq,
        AVG(price) OVER (PARTITION BY product_id, store_id ORDER BY revenue ASC, id ASC ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS price_avg_6
    FROM joined
    WINDOW w_recent AS (PARTITION BY store_id ORDER BY revenue ASC, id ASC)
),
with_windows AS (
    SELECT
        id,
        product_id,
        units,
        revenue,
        launch_date,
        price,
        stock_qty,
        next_stock_qty,
        prev_units,
        row_seq,
        price_avg_6,
        CORR(units, revenue) OVER (PARTITION BY YEAR(launch_date), store_id) AS corr_units_revenue,
        AVG(next_units) OVER (PARTITION BY product_id, store_id ORDER BY launch_date ASC, id ASC ROWS BETWEEN 5 PRECEDING AND 1 PRECEDING) AS next_units_avg_5_prior,
        CORR(revenue, units) OVER (PARTITION BY MONTH(launch_date), store_id) AS corr_revenue_units
    FROM metrics
)
SELECT
    CASE WHEN stock_qty >= 409 THEN 'LARGE' ELSE 'SMALL' END AS stock_qty_tier,
    COUNT(*) AS event_count,
    ROUND(AVG(row_seq), 2) AS avg_row_seq,
    PERCENTILE_APPROX(stock_qty, 0.9) AS stock_qty_p90
FROM with_windows
GROUP BY stock_qty_tier
HAVING avg_row_seq > 0
