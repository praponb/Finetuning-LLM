WITH base AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        carrier,
        shipped_date,
        delivered_date,
        cost,
        DATEDIFF(CURRENT_DATE(), shipped_date) AS days_since_shipped_date,
        CONCAT(carrier, ' ', status) AS carrier_status,
        COALESCE(carrier, 'N/A') AS carrier_clean
    FROM shipments
    WHERE carrier IS NOT NULL
),
valid AS (
    -- Basic cleansing and defaults
    SELECT
        *,
        COALESCE(cost, 0) AS cost_filled,
        COALESCE(carrier, 'OTHER') AS carrier_clean_2
    FROM base
    WHERE cost BETWEEN 30 AND 48
      AND carrier IS NOT NULL
),
latest AS (
    -- Deduplicate with ROW_NUMBER
    SELECT
        id,
        carrier,
        carrier_status,
        cost_filled
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY carrier ORDER BY days_since_shipped_date DESC, id) AS rn
        FROM valid
    ) x
    WHERE rn = 1
),
latest_shipments AS (
    -- QUALIFY-style window filter
    SELECT *, ROW_NUMBER() OVER (PARTITION BY carrier ORDER BY cost_filled DESC, id) AS rn
    FROM latest
    QUALIFY rn = 1
)
-- Aggregate with conditional metrics
SELECT
    carrier,
    COUNT(*) AS num_rows,
    COUNT(CASE WHEN carrier = 'DHL' THEN 1 END) AS dhl_count,
    APPROX_PERCENTILE(cost_filled, 0.95) AS cost_filled_p95,
    SUM(CASE WHEN carrier = 'UPS' THEN cost_filled ELSE 0 END) AS ups_cost_filled,
    COUNT(*) FILTER (WHERE carrier = 'Flash') AS flash_rows
FROM latest_shipments
GROUP BY carrier;
