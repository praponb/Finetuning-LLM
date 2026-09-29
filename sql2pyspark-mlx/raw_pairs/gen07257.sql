WITH base AS (
    -- Keep recent, valid rows
    SELECT
        id,
        order_id,
        carrier,
        delivered_date,
        cost,
        DATE_FORMAT(delivered_date, 'MMM yyyy') AS delivered_date_label,
        COALESCE(carrier, 'NONE') AS resolved_carrier
    FROM shipments
    WHERE shipped_date > TO_DATE('2023-02-17')
      AND carrier IS NOT NULL
),
with_windows AS (
    -- Ranking and gap features
    SELECT
        id,
        order_id,
        cost,
        delivered_date_label,
        MIN(cost) OVER (PARTITION BY order_id, carrier ORDER BY cost, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_min_cost,
        SUM(cost) OVER (PARTITION BY order_id, carrier ORDER BY cost, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_sum_cost,
        AVG(cost) OVER (PARTITION BY order_id, carrier ORDER BY cost, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_avg_cost
    FROM base
),
latest AS (
    -- Deduplicate with ROW_NUMBER
    SELECT
        id,
        delivered_date_label,
        running_min_cost,
        running_avg_cost
    FROM (
        SELECT *, RANK() OVER (PARTITION BY order_id ORDER BY running_sum_cost DESC) AS latest_rank
        FROM with_windows
    ) ranked
    WHERE latest_rank = 1
),
base_shipments AS (
    -- Filter the raw rows and derive helper columns
    SELECT *, CAST(running_min_cost AS INT) AS running_min_cost_int
    FROM latest
    WHERE running_avg_cost < 39
)
SELECT
    id,
    delivered_date_label,
    running_min_cost,
    running_avg_cost,
    running_min_cost_int
FROM base_shipments
ORDER BY running_min_cost DESC NULLS LAST
