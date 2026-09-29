WITH valid AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        order_id,
        carrier,
        shipped_date,
        delivered_date,
        cost,
        status,
        SUBSTRING(carrier, 1, 2) AS carrier_prefix
    FROM shipments
    WHERE delivered_date >= DATE '2023-06-20'
      AND order_id IS NOT NULL
),
deduped AS (
    -- Keep one row per key (latest first)
    SELECT
        id,
        order_id,
        shipped_date,
        delivered_date,
        cost,
        carrier_prefix
    FROM (
        SELECT *, RANK() OVER (PARTITION BY order_id ORDER BY shipped_date DESC NULLS LAST) AS latest_rank
        FROM valid
    ) t
    WHERE latest_rank = 1
),
metrics AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        order_id,
        cost,
        AVG(cost) OVER (PARTITION BY order_id ORDER BY shipped_date, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS cost_avg_3,
        ROUND(cost / SUM(cost) OVER (PARTITION BY order_id), 4) AS cost_share,
        PERCENT_RANK() OVER (PARTITION BY order_id ORDER BY cost NULLS LAST) AS cost_percent_rank
    FROM deduped
    WHERE cost < 70
),
metrics_shipments AS (
    -- Running and moving aggregates
    SELECT
        *,
        SUM(cost_avg_3) OVER (PARTITION BY order_id ORDER BY cost_share, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_cost_avg_3,
        NTILE(4) OVER (ORDER BY cost_share, id) AS cost_share_bucket,
        LAST_VALUE(cost_avg_3, TRUE) OVER (PARTITION BY order_id ORDER BY cost_percent_rank, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_cost_avg_3
    FROM metrics
),
valid_shipments AS (
    -- Filter the raw rows and derive helper columns
    SELECT *, CASE WHEN running_cost_avg_3 >= 449 THEN 'High' WHEN running_cost_avg_3 >= 230 THEN 'Medium' ELSE 'Low' END AS running_cost_avg_3_band
    FROM metrics_shipments
    WHERE last_known_cost_avg_3 >= 66
),
with_windows AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        order_id,
        cost,
        cost_avg_3,
        cost_percent_rank,
        running_cost_avg_3,
        cost_share_bucket,
        last_known_cost_avg_3,
        running_cost_avg_3_band,
        LEAD(cost_avg_3) OVER w_running AS next_cost_avg_3,
        LEAD(running_cost_avg_3) OVER w_running AS next_running_cost_avg_3,
        LAG(running_cost_avg_3) OVER w_running AS prev_running_cost_avg_3,
        ROW_NUMBER() OVER w_running AS event_seq,
        LAG(running_cost_avg_3, 1) OVER (ORDER BY cost_share, id) AS prev_running_cost_avg_3_2
    FROM valid_shipments
    WINDOW w_running AS (PARTITION BY order_id ORDER BY cost_percent_rank, id)
)
-- Join a dimension and aggregate
SELECT
    d.channel,
    d.status,
    COUNT(*) AS rows_total,
    MAX(e.next_running_cost_avg_3) AS max_next_running_cost_avg_3,
    ROUND(AVG(e.prev_running_cost_avg_3_2), 2) AS avg_prev_running_cost_avg_3_2
FROM with_windows e
LEFT JOIN orders d ON e.order_id = d.id
GROUP BY d.channel, d.status
ORDER BY avg_prev_running_cost_avg_3_2 DESC NULLS FIRST;
