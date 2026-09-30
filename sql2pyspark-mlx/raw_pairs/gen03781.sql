WITH filtered AS (
    -- Keep recent, valid rows
    SELECT
        *,
        LENGTH(status) AS status_length,
        COALESCE(carrier, 'DIRECT') AS carrier_filled
    FROM shipments
    WHERE order_id IS NOT NULL
      AND shipped_date BETWEEN '2024-02-01' AND '2025-01-29'
),
cleaned AS (
    -- Basic cleansing and defaults
    SELECT
        *,
        cost * status_length AS cost_times_status_length,
        COALESCE(carrier, 'N/A') AS resolved_carrier
    FROM filtered
    WHERE carrier IN ('FedEx', 'Flash')
),
valid AS (
    -- Basic cleansing and defaults
    SELECT
        *,
        DATEDIFF(CURRENT_DATE(), delivered_date) / 30 AS months_since_delivered_date,
        COALESCE(cost, 0) AS cost_filled
    FROM cleaned
    WHERE shipped_date >= DATE '2023-05-29'
      AND YEAR(shipped_date) = 2023
),
deduped AS (
    -- Deduplicate with ROW_NUMBER
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY status ORDER BY delivered_date, id) AS dedup_rank
        FROM valid
    ) t
    WHERE dedup_rank = 1
)
-- Aggregate with conditional metrics
SELECT
    DATE_TRUNC('YEAR', delivered_date) AS year_start,
    carrier,
    DATE_TRUNC('QUARTER', shipped_date) AS quarter_start,
    ROUND(AVG(status_length), 2) AS avg_status_length,
    SUM(CASE WHEN carrier = 'UPS' THEN cost ELSE 0 END) AS ups_cost
FROM deduped
GROUP BY GROUPING SETS ((DATE_TRUNC('YEAR', delivered_date), carrier, DATE_TRUNC('QUARTER', shipped_date)), (DATE_TRUNC('YEAR', delivered_date), carrier), ())
HAVING avg_status_length >= 0
ORDER BY quarter_start DESC NULLS FIRST;
