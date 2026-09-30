WITH valid_sites AS (
    -- Filter sites
    SELECT
        id,
        site_name,
        zone,
        tariff,
        installed_kw,
        commissioned_on
    FROM sites
    WHERE installed_kw > 141.6
      AND zone IN ('Z2', 'Z4')
),
with_activity AS (
    -- Rows that have matching meter_readings
    SELECT s.*
    FROM valid_sites s
    WHERE EXISTS (
            SELECT 1
            FROM meter_readings m
            WHERE m.site_id = s.id
              AND m.kwh < 113.0
              AND m.quality IN ('OK', 'EST')
        )
),
derived AS (
    -- Derive helper columns
    SELECT
        *,
        CASE WHEN tariff IN ('FLAT', 'TOU') THEN 'group_a' WHEN tariff = 'EV' THEN 'group_b' ELSE 'other' END AS tariff_group
    FROM with_activity
    WHERE tariff IN ('TOU', 'EV')
),
sequenced AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        ROW_NUMBER() OVER (PARTITION BY DATE_TRUNC('MONTH', commissioned_on), tariff ORDER BY commissioned_on DESC, id) AS rn,
        ROUND(AVG(installed_kw) OVER (PARTITION BY tariff_group), 2) AS tariff_group_avg_installed_kw,
        MAX(installed_kw) OVER (PARTITION BY tariff_group) AS tariff_group_max_installed_kw,
        MIN(installed_kw) OVER w_tariff_group AS tariff_group_min_installed_kw
    FROM derived
    WINDOW w_tariff_group AS (PARTITION BY tariff_group)
),
line_level_readings AS (
    -- One row per reading
    SELECT
        s.id,
        s.zone,
        s.tariff,
        s.rn,
        s.tariff_group_avg_installed_kw,
        s.tariff_group_min_installed_kw,
        m.id AS reading_id,
        m.kwh AS reading_kwh,
        m.quality AS reading_quality,
        m.read_at AS reading_read_at
    FROM sequenced s
    JOIN meter_readings m ON m.site_id = s.id
),
screened AS (
    -- Rows with no matching meter_readings
    SELECT s.*
    FROM line_level_readings s
    WHERE NOT EXISTS (
            SELECT 1
            FROM meter_readings m
            WHERE m.site_id = s.id
              AND m.voltage > 214.3
        )
)
SELECT
    reading_quality,
    tariff,
    rn,
    MIN(reading_kwh) AS min_reading_kwh
FROM screened
GROUP BY CUBE(reading_quality, tariff, rn)
ORDER BY reading_quality NULLS LAST, tariff NULLS LAST, rn NULLS LAST
