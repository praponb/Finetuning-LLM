WITH base AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        *,
        CASE WHEN cost >= 96 THEN 'High' WHEN cost >= 32 THEN 'Medium' ELSE 'Low' END cost_tier,
        COALESCE(carrier, 'OTHER') carrier_filled
    FROM shipments
    WHERE delivered_date > TO_DATE('2023-06-06')
      AND order_id IS NOT NULL
),
base_shipments AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        *,
        DATEDIFF(CURRENT_DATE(), shipped_date) days_since_shipped_date,
        COALESCE(carrier, 'OTHER') resolved_carrier
    FROM base
    WHERE delivered_date > DATE '2023-06-23'
      AND carrier IS NOT NULL
),
cleaned AS (
    -- Keep recent, valid rows
    SELECT
        *,
        TRIM(carrier) clean_carrier,
        COALESCE(carrier, 'OTHER') carrier_filled_2
    FROM base_shipments
    WHERE order_id = 4
),
valid AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        *,
        DATEDIFF(delivered_date, shipped_date) / 7 weeks_shipped_date_to_delivered_date,
        UPPER(carrier) carrier_upper
    FROM cleaned
    WHERE status NOT IN ('LOST', 'RETURNED', 'DELIVERED')
)
-- Join a dimension and aggregate
SELECT
    o.channel,
    a.carrier,
    COUNT(*) AS rows_total,
    MAX(a.cost) AS max_cost,
    AVG(a.days_since_shipped_date) AS avg_days_since_shipped_date
FROM valid a
INNER JOIN orders o ON a.order_id = o.id
GROUP BY o.channel, a.carrier
ORDER BY carrier
