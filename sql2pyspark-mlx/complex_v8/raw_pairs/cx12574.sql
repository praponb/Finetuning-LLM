WITH scoped AS (
    -- Filter meter_readings
    SELECT
        id,
        site_id,
        read_at,
        kwh,
        voltage,
        quality,
        SUBSTRING(TRIM(quality), 1, 4) AS quality_prefix
    FROM meter_readings
    WHERE TO_DATE(read_at) < DATE '2025-02-06'
),
enriched_site AS (
    -- Join sites
    SELECT
        s.id,
        s.site_id,
        s.read_at,
        s.kwh,
        s.voltage,
        s.quality,
        si.tariff AS site_tariff,
        si.site_name,
        si.commissioned_on AS site_commissioned_on
    FROM scoped s
    JOIN sites si ON si.id = s.site_id
),
filtered AS (
    -- Rows with no matching meter_readings
    SELECT s.*
    FROM enriched_site s
    WHERE NOT EXISTS (
            SELECT 1
            FROM meter_readings m
            WHERE m.site_id = s.site_id
              AND m.kwh <= 81.9
        )
      AND s.site_id IS NOT NULL
),
metrics AS (
    -- Window metrics over several partitions
    SELECT
        id,
        read_at,
        kwh,
        voltage,
        quality,
        site_name,
        site_commissioned_on,
        MIN(kwh) OVER (PARTITION BY site_id, DATE_TRUNC('MONTH', site_commissioned_on) ORDER BY kwh DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS min_kwh_so_far,
        MAX(kwh) OVER (PARTITION BY site_tariff, quality ORDER BY kwh DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS max_kwh_so_far,
        ROUND((UNIX_TIMESTAMP(read_at) - UNIX_TIMESTAMP(LAG(read_at) OVER (PARTITION BY site_tariff, quality ORDER BY kwh DESC, id))) / 60, 1) AS minutes_since_prev_read_at,
        SUM(kwh) OVER (PARTITION BY site_id, DATE_TRUNC('MONTH', site_commissioned_on) ORDER BY kwh DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_kwh
    FROM filtered
),
derived AS (
    -- Label and score rows
    SELECT
        *,
        CASE WHEN max_kwh_so_far >= 960.0 THEN 'A' WHEN max_kwh_so_far >= 480.0 THEN 'B' ELSE 'C' END AS max_kwh_so_far_tier,
        DATEDIFF(DATE '2025-12-31', site_commissioned_on) AS days_since_site_commissioned_on
    FROM metrics
    WHERE max_kwh_so_far < 720.0
)
-- Native SQL UNPIVOT (wide to long)
SELECT id, measure, amount
FROM (
    SELECT id, kwh, voltage
    FROM derived
) src
UNPIVOT (
    amount FOR measure IN (kwh, voltage)
) unp
