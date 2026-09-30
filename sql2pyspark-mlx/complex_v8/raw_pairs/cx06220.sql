WITH prepared_readings AS (
    -- Filter meter_readings
    SELECT
        id,
        site_id,
        read_at,
        voltage,
        quality
    FROM meter_readings
    WHERE voltage < 240.2
      AND quality IN ('OK', 'BAD')
),
enriched AS (
    -- Attach site attributes
    SELECT
        s.id,
        s.site_id,
        s.read_at,
        s.voltage,
        s.quality,
        si.site_name
    FROM prepared_readings s
    LEFT JOIN sites si ON si.id = s.site_id AND si.commissioned_on BETWEEN DATE '2024-06-27' AND DATE '2025-11-28'
),
metrics AS (
    -- Running and moving aggregates
    SELECT
        id,
        site_id,
        read_at,
        quality,
        ROUND(CUME_DIST() OVER (ORDER BY read_at DESC), 3) AS read_at_cume_dist,
        LAG(read_at, 2) OVER (PARTITION BY site_id ORDER BY read_at, id) AS prev2_read_at,
        LAG(read_at) OVER (ORDER BY read_at DESC, id) AS prev_read_at,
        LAST_VALUE(read_at) OVER (ORDER BY read_at DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_read_at,
        ROW_NUMBER() OVER (ORDER BY voltage, id) AS rn
    FROM enriched
),
filtered AS (
    -- Rows with no matching meter_readings
    SELECT s.*
    FROM metrics s
    WHERE NOT EXISTS (
            SELECT 1
            FROM meter_readings m
            WHERE m.site_id = s.site_id
              AND m.voltage <= 232.4
        )
),
deduped AS (
    -- Keep the earliest row per site_id, quality
    SELECT
        id,
        site_id,
        read_at,
        quality,
        read_at_cume_dist,
        prev2_read_at,
        prev_read_at,
        last_read_at,
        rn
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY site_id, quality ORDER BY prev2_read_at, id) AS rn_2
        FROM filtered
    ) ranked
    WHERE rn_2 = 1
)
-- Subtotals with ROLLUP
SELECT
    COALESCE(quality, 'ALL') AS quality,
    site_id,
    SUM(read_at_cume_dist) AS total_read_at_cume_dist,
    COUNT(*) AS cnt
FROM deduped
GROUP BY ROLLUP(quality, site_id)
