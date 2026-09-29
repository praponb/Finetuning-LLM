WITH filtered AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        carrier,
        shipped_date,
        delivered_date,
        cost,
        status,
        CASE WHEN carrier IN ('UPS', 'DHL') THEN 'Group A' ELSE 'Other' END AS carrier_group,
        CONCAT(status, ', ', carrier) AS status_carrier,
        COALESCE(carrier, 'NONE') AS carrier_filled
    FROM shipments
    WHERE delivered_date > DATE '2023-03-13'
      AND carrier IS NOT DISTINCT FROM 'UPS'
),
base AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        carrier,
        shipped_date,
        delivered_date,
        cost,
        status,
        carrier_group,
        status_carrier,
        carrier_filled,
        ADD_MONTHS(delivered_date, 12) AS delivered_date_plus_months
    FROM filtered
    WHERE carrier = 'Kerry'
),
deduped AS (
    -- Keep one row per key (latest first)
    SELECT *
    FROM base
    QUALIFY ROW_NUMBER() OVER (PARTITION BY status ORDER BY delivered_date_plus_months NULLS LAST, id) = 1
),
latest AS (
    -- QUALIFY-style window filter
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY status, carrier ORDER BY shipped_date DESC, id) AS rn
        FROM deduped
    ) ranked
    WHERE rn = 1
)
-- Multi-level aggregation
SELECT
    DATE_TRUNC('MONTH', delivered_date_plus_months) AS activity_month,
    COUNT(*) FILTER (WHERE status = 'DELIVERED') AS delivered_rows,
    ROUND(AVG(cost), 2) AS avg_cost
FROM latest
GROUP BY 1
ORDER BY delivered_rows DESC NULLS FIRST;
