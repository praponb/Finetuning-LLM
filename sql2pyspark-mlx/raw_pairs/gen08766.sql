WITH enriched AS (
    SELECT e.*, d.order_date
    FROM shipments e
    INNER JOIN orders d
        ON e.order_id = d.id
),
with_windows AS (
    SELECT
        *,
        MAX(cost) OVER (PARTITION BY status, carrier ORDER BY shipped_date, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) last7_max_cost,
        SUM(cost) OVER (PARTITION BY status, carrier ORDER BY shipped_date, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) last7_sum_cost,
        AVG(cost) OVER (PARTITION BY status, carrier ORDER BY shipped_date, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) last7_avg_cost,
        COUNT(cost) OVER (PARTITION BY status, carrier ORDER BY shipped_date, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) last7_count_cost
    FROM enriched
    WHERE YEAR(delivered_date) >= 2024
),
metrics AS (
    SELECT
        *,
        SUM(last7_max_cost) OVER (ORDER BY last7_avg_cost, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) cumulative_last7_max_cost,
        LAG(last7_count_cost, 2) OVER (PARTITION BY order_id ORDER BY last7_sum_cost, id) prev_last7_count_cost
    FROM with_windows
),
enriched_shipments AS (
    SELECT
        *,
        LAG(prev_last7_count_cost) OVER w_recent prev_prev_last7_count_cost,
        LEAD(prev_last7_count_cost) OVER w_recent next_prev_last7_count_cost,
        LEAD(last7_count_cost) OVER w_recent next_last7_count_cost,
        ROW_NUMBER() OVER w_recent row_seq,
        CUME_DIST() OVER w_grp cumulative_last7_max_cost_cume_dist
    FROM metrics
    WINDOW w_recent AS (PARTITION BY carrier ORDER BY cost, id), w_grp AS (ORDER BY cumulative_last7_max_cost NULLS FIRST)
),
with_dims AS (
    SELECT
        src.id,
        src.order_id,
        src.carrier,
        src.shipped_date,
        src.delivered_date,
        src.last7_sum_cost,
        src.last7_count_cost,
        src.cumulative_last7_max_cost,
        src.prev_prev_last7_count_cost,
        src.next_prev_last7_count_cost,
        d.order_date AS order_order_date,
        d.amount
    FROM enriched_shipments src
    INNER JOIN orders d
        ON src.order_id = d.id
),
grouped AS (
    SELECT
        CASE WHEN last7_sum_cost >= 400 THEN 'HIGH' ELSE 'LOW' END AS last7_sum_cost_tier,
        order_id,
        carrier,
        APPROX_PERCENTILE(next_prev_last7_count_cost, 0.5) AS next_prev_last7_count_cost_p50,
        AVG(prev_prev_last7_count_cost) FILTER (WHERE carrier = 'FedEx') AS fedex_avg_prev_prev_last7_count_cost
    FROM with_dims
    GROUP BY GROUPING SETS ((CASE WHEN last7_sum_cost >= 400 THEN 'HIGH' ELSE 'LOW' END, order_id, carrier), (CASE WHEN last7_sum_cost >= 400 THEN 'HIGH' ELSE 'LOW' END, order_id), (CASE WHEN last7_sum_cost >= 400 THEN 'HIGH' ELSE 'LOW' END))
)
SELECT
    order_id,
    carrier,
    next_prev_last7_count_cost_p50
FROM grouped
ORDER BY carrier DESC NULLS FIRST
