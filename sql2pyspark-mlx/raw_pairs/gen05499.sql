WITH valid AS (
    SELECT
        id,
        carrier,
        shipped_date,
        delivered_date,
        status,
        ROUND(cost, 2) AS cost_rounded,
        CONCAT(carrier, ' ', status) AS carrier_status
    FROM shipments
    WHERE delivered_date >= TO_DATE('2023-06-01')
      AND shipped_date <= '2024-12-30'
),
matched_keys AS (
    SELECT carrier
    FROM valid
    EXCEPT ALL
    SELECT carrier
    FROM shipments
    WHERE order_id IS NULL
),
verified AS (
    SELECT s.*
    FROM valid s
    JOIN matched_keys k ON s.carrier = k.carrier
),
filtered AS (
    SELECT
        *,
        UPPER(carrier) AS carrier_upper,
        CASE WHEN cost_rounded >= 89 THEN 'High' WHEN cost_rounded >= 15 THEN 'Medium' ELSE 'Low' END AS cost_rounded_level,
        COALESCE(carrier, 'N/A') AS carrier_filled
    FROM verified
    WHERE delivered_date >= DATE '2023-06-03'
      AND shipped_date >= '2023-07-19'
),
remaining_keys AS (
    SELECT status, carrier
    FROM filtered
    EXCEPT
    SELECT status, carrier
    FROM shipments
    WHERE cost BETWEEN 56 AND 65
),
reconciled AS (
    SELECT s.*
    FROM filtered s
    JOIN remaining_keys k ON s.status = k.status AND s.carrier = k.carrier
),
with_windows AS (
    SELECT *, CUME_DIST() OVER (PARTITION BY carrier ORDER BY cost_rounded) AS cost_rounded_cume_dist
    FROM reconciled
),
scored AS (
    SELECT
        delivered_date,
        cost_rounded,
        carrier_status,
        RANK() OVER (ORDER BY cost_rounded NULLS LAST) AS cost_rounded_rank
    FROM with_windows
    WHERE shipped_date <= '2025-07-01'
),
base AS (
    SELECT
        *,
        CASE WHEN cost_rounded >= 66 THEN 'High' WHEN cost_rounded >= 10 THEN 'Medium' ELSE 'Low' END AS cost_rounded_level,
        DATEDIFF(CURRENT_DATE(), delivered_date) / 365 AS years_since_delivered_date
    FROM scored
    WHERE delivered_date > DATE '2023-08-16'
      AND cost_rounded_rank > 3
)
SELECT
    cost_rounded,
    carrier_status,
    cost_rounded_rank
FROM base
ORDER BY cost_rounded_rank DESC;
