WITH metrics AS (
    SELECT
        *,
        NTILE(4) OVER (PARTITION BY status ORDER BY cost, id) cost_bucket,
        LEAD(cost, 1) OVER (PARTITION BY carrier ORDER BY shipped_date, id) next_cost
    FROM shipments
),
filtered AS (
    SELECT
        id,
        carrier,
        shipped_date,
        delivered_date,
        next_cost,
        INITCAP(TRIM(carrier)) carrier_title
    FROM metrics
    WHERE order_id IS NOT NULL
),
ranked AS (
    SELECT *
    FROM filtered
    QUALIFY ROW_NUMBER() OVER (PARTITION BY carrier ORDER BY shipped_date, id) = 1
),
remaining_keys AS (
    SELECT id, carrier
    FROM ranked
    EXCEPT
    SELECT id, carrier
    FROM shipments
    WHERE delivered_date > '2025-01-28'
),
verified AS (
    SELECT s.*
    FROM ranked s
    INNER JOIN remaining_keys k ON s.id = k.id AND s.carrier = k.carrier
)
SELECT
    carrier,
    MAX(next_cost) AS max_next_cost,
    COUNT(CASE WHEN carrier = 'UPS' THEN 1 END) AS ups_count,
    PERCENTILE_APPROX(next_cost, ARRAY(0.25, 0.5, 0.75)) AS next_cost_quartiles,
    ROUND(SUM(next_cost) / NULLIF(COUNT(CASE WHEN carrier = 'DHL' THEN 1 END), 0), 2) AS next_cost_per_dhl,
    ROUND(AVG(next_cost), 2) AS avg_next_cost
FROM verified
GROUP BY 1
