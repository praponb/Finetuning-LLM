WITH cleaned AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        *,
        QUARTER(launch_date) AS launch_date_quarter,
        CASE WHEN brand = 'Acme' THEN 1 ELSE 0 END AS is_acme
    FROM products
    WHERE launch_date >= DATE '2023-09-05'
      AND supplier_id IS NOT NULL
),
enriched AS (
    -- Running and moving aggregates
    SELECT
        id,
        product_name,
        stock_qty,
        launch_date,
        launch_date_quarter,
        ROW_NUMBER() OVER (PARTITION BY category ORDER BY cost DESC, id) AS event_seq,
        ROUND(cost / SUM(cost) OVER (PARTITION BY supplier_id), 4) AS cost_share
    FROM cleaned
    WHERE is_acme < 62
),
with_windows AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        AVG(cost_share) OVER w_running AS running_avg_cost_share,
        SUM(cost_share) OVER w_running AS running_sum_cost_share,
        MIN(cost_share) OVER w_running AS running_min_cost_share
    FROM enriched
    WINDOW w_running AS (ORDER BY launch_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
),
with_last AS (
    -- Latest matching store_sale per row (LATERAL ... LIMIT 1)
    SELECT
        s.id,
        s.product_name,
        s.event_seq,
        s.running_avg_cost_share,
        s.running_sum_cost_share,
        latest.sale_date
    FROM with_windows s
    LEFT JOIN LATERAL (
            SELECT c.sale_date
            FROM store_sales c
            WHERE c.product_id = s.id
            ORDER BY c.units DESC, c.id DESC
            LIMIT 1
        ) latest ON TRUE
),
matched_keys AS (
    -- Set intersection on keys
    SELECT id
    FROM with_last
    INTERSECT ALL
    SELECT id
    FROM products
    WHERE supplier_id IS NULL
),
verified AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM with_last s
    INNER JOIN matched_keys k ON s.id = k.id
)
-- Multi-level aggregation
SELECT
    CASE WHEN event_seq >= 9 THEN 'LARGE' ELSE 'SMALL' END AS event_seq_tier,
    DATE_TRUNC('WEEK', sale_date) AS week_start,
    COUNT(DISTINCT id) AS distinct_ids,
    PERCENTILE_APPROX(running_sum_cost_share, 0.9) AS running_sum_cost_share_p90,
    MIN(event_seq) AS min_event_seq,
    APPROX_PERCENTILE(running_avg_cost_share, ARRAY(0.25, 0.5, 0.75)) AS running_avg_cost_share_quartiles,
    MIN(event_seq) AS min_event_seq_2,
    ROUND(AVG(running_avg_cost_share), 2) AS avg_running_avg_cost_share
FROM verified
GROUP BY 1, 2
ORDER BY running_sum_cost_share_p90 DESC;
