WITH filtered AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        site_id,
        read_at,
        kwh,
        voltage
    FROM meter_readings
    WHERE (quality = 'BAD' OR read_at >= TIMESTAMP '2025-02-06 12:00:00')
),
stats AS (
    -- Summarise per group
    SELECT
        site_id,
        YEAR(read_at) AS read_at_year,
        COUNT(*) AS n_rows,
        MAX(read_at) AS last_read_at,
        MIN(read_at) AS first_read_at,
        ROUND(AVG(voltage), 2) AS avg_voltage
    FROM filtered
    GROUP BY site_id, YEAR(read_at)
    HAVING SUM(voltage) > 222.5
),
annotated AS (
    -- Add calculated columns
    SELECT
        *,
        ROUND(n_rows / NULLIF(read_at_year, 0), 3) AS n_rows_per_read_at_year,
        last_read_at + INTERVAL 7 DAYS AS last_read_at_plus_7d,
        DATE_TRUNC('WEEK', last_read_at) AS last_read_at_week
    FROM stats
    WHERE MONTH(last_read_at) IN (1, 3)
)
-- Final projection
SELECT
    site_id,
    read_at_year,
    n_rows,
    last_read_at,
    avg_voltage,
    last_read_at_plus_7d,
    last_read_at_week,
    MONTH(first_read_at) AS first_read_at_month
FROM annotated
WHERE site_id IS NOT NULL
ORDER BY read_at_year DESC NULLS FIRST
