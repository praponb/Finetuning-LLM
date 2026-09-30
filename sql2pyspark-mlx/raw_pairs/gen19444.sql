WITH latest AS (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY status, order_id ORDER BY cost DESC NULLS FIRST, id) AS rn
    FROM shipments
    QUALIFY rn = 1
),
with_windows AS (
    SELECT
        id,
        carrier,
        shipped_date,
        delivered_date,
        cost,
        ROUND(cost / SUM(cost) OVER (PARTITION BY order_id), 4) AS cost_share,
        SUM(cost) OVER w_hist AS cumulative_cost,
        LAG(cost, 1) OVER (ORDER BY rn, id) AS prev_cost,
        SUM(cost) OVER (PARTITION BY order_id ORDER BY delivered_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_cost,
        LEAD(cost, 1, 0) OVER (PARTITION BY carrier ORDER BY delivered_date, id) AS next_cost
    FROM latest
    WHERE cost BETWEEN 59 AND 90
    WINDOW w_hist AS (PARTITION BY carrier ORDER BY shipped_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
),
ranked AS (
    SELECT *
    FROM with_windows
    QUALIFY ROW_NUMBER() OVER (PARTITION BY carrier ORDER BY cost NULLS LAST, id) = 1
),
latest_shipments AS (
    SELECT
        id,
        shipped_date,
        delivered_date,
        cost,
        prev_cost,
        running_cost
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY carrier ORDER BY cumulative_cost DESC NULLS LAST, id) AS rank_in_group
        FROM ranked
    ) t
    WHERE rank_in_group = 1
),
filtered AS (
    SELECT *, DATEDIFF(shipped_date, delivered_date) AS days_delivered_date_to_shipped_date
    FROM latest_shipments
    WHERE shipped_date >= DATE '2023-06-27'
      AND cost BETWEEN 37 AND 71
),
valid AS (
    SELECT *, DAYOFWEEK(shipped_date) AS shipped_date_dayofweek
    FROM filtered
    WHERE delivered_date > DATE '2023-06-05'
      AND days_delivered_date_to_shipped_date >= 39
),
metrics AS (
    SELECT
        *,
        LEAD(prev_cost, 2) OVER (ORDER BY running_cost, id) AS next_prev_cost,
        NTILE(3) OVER (ORDER BY prev_cost DESC, id) AS prev_cost_bucket,
        LEAD(running_cost, 1) OVER (ORDER BY delivered_date, id) AS next_running_cost
    FROM valid
),
summary AS (
    SELECT
        DATE_TRUNC('WEEK', delivered_date) AS week_start,
        APPROX_PERCENTILE(running_cost, 0.95) AS running_cost_p95,
        ROUND(AVG(next_running_cost), 2) AS avg_next_running_cost,
        COUNT(DISTINCT id) AS distinct_ids
    FROM metrics
    GROUP BY 1
)
SELECT
    week_start,
    avg_next_running_cost,
    distinct_ids
FROM summary
ORDER BY week_start NULLS FIRST
