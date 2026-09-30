WITH cleaned AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        site_name,
        installed_kw,
        commissioned_on,
        LOWER(TRIM(zone)) AS zone_clean
    FROM sites
    WHERE zone NOT IN ('Z2', 'Z3')
      AND installed_kw <= 373.3
),
ranked AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        COUNT(*) OVER w_commissioned_on_month AS rows_in_commissioned_on_month,
        LAG(installed_kw, 3, 0) OVER (ORDER BY commissioned_on, id) AS prev3_installed_kw,
        MIN(installed_kw) OVER (PARTITION BY MONTH(commissioned_on)) AS commissioned_on_month_min_installed_kw
    FROM cleaned
    WINDOW w_commissioned_on_month AS (PARTITION BY MONTH(commissioned_on))
),
exploded_detail_readings AS (
    -- One row per reading
    SELECT
        s.id,
        s.commissioned_on,
        s.zone_clean,
        s.prev3_installed_kw,
        m.id AS reading_id,
        m.kwh AS reading_kwh,
        m.read_at AS reading_read_at
    FROM ranked s
    JOIN meter_readings m ON m.site_id = s.id
)
-- Native SQL UNPIVOT (wide to long)
SELECT id, reading_id, metric, amount
FROM (
    SELECT id, reading_id, prev3_installed_kw, reading_kwh
    FROM exploded_detail_readings
) src
UNPIVOT INCLUDE NULLS (
    amount FOR metric IN (prev3_installed_kw, reading_kwh)
) unp
ORDER BY id, reading_id, metric
