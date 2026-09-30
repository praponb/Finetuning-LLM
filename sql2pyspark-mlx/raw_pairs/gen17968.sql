WITH metrics AS (
    -- Ranking and gap features
    SELECT
        id,
        order_id,
        COUNT(cost) OVER w_ordered AS running_count_cost,
        MIN(cost) OVER w_ordered AS running_min_cost,
        SUM(cost) OVER w_ordered AS running_sum_cost
    FROM shipments
    WINDOW w_ordered AS (PARTITION BY order_id, status ORDER BY shipped_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
),
valid AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        order_id,
        running_count_cost,
        running_min_cost,
        running_sum_cost,
        CASE WHEN running_min_cost >= 76 THEN 'High' WHEN running_min_cost >= 30 THEN 'Medium' ELSE 'Low' END AS running_min_cost_level
    FROM metrics
    WHERE order_id = 1
),
scored AS (
    -- Running and moving aggregates
    SELECT
        id,
        order_id,
        running_count_cost,
        running_sum_cost,
        running_min_cost_level,
        LAG(running_count_cost, 1) OVER w_running AS prev_running_count_cost,
        AVG(running_count_cost) OVER w_hist AS order_id_avg_running_count_cost,
        MIN(running_sum_cost) OVER w_hist AS order_id_min_running_sum_cost
    FROM valid
    WHERE order_id IS NULL
    WINDOW w_running AS (PARTITION BY order_id ORDER BY running_min_cost, id), w_hist AS (PARTITION BY order_id)
),
with_dims AS (
    -- Attach orders attributes
    SELECT
        s.id,
        s.order_id,
        s.running_count_cost,
        s.running_sum_cost,
        s.running_min_cost_level,
        s.prev_running_count_cost,
        s.order_id_avg_running_count_cost,
        s.order_id_min_running_sum_cost,
        p.channel,
        p.order_date
    FROM scored s
    LEFT JOIN orders p
        ON s.order_id <=> p.id
)
-- Join a dimension and aggregate
SELECT
    o.status,
    o.channel AS order_channel,
    a.channel AS shipment_channel,
    COUNT(*) AS cnt,
    AVG(a.prev_running_count_cost) AS avg_prev_running_count_cost,
    MAX(a.running_sum_cost) AS max_running_sum_cost,
    GROUPING_ID() AS gid
FROM with_dims a
LEFT JOIN orders o ON a.order_id = o.id
GROUP BY GROUPING SETS ((o.status, o.channel, a.channel), (o.status, o.channel), (o.status), ())
ORDER BY avg_prev_running_count_cost
