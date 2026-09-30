WITH scoped AS (
    -- Scope the meter_readings rows
    SELECT
        id,
        site_id,
        kwh,
        voltage,
        quality,
        DAYOFWEEK(read_at) AS read_at_dayofweek
    FROM meter_readings
    WHERE read_at >= TIMESTAMP '2025-02-06 10:00:00'
),
with_parent AS (
    -- Attach site attributes
    SELECT
        s.id,
        s.site_id,
        s.voltage,
        s.quality,
        s.read_at_dayofweek,
        si.tariff AS site_tariff
    FROM scoped s
    LEFT JOIN sites si ON si.id = s.site_id AND si.installed_kw > 96.9
),
enriched AS (
    -- Label and score rows
    SELECT
        *,
        GREATEST(read_at_dayofweek, voltage) AS greatest_read_at_dayofweek_voltage,
        CONCAT_WS(' / ', quality, site_tariff) AS quality_site_tariff_label,
        LENGTH(TRIM(quality)) AS quality_len
    FROM with_parent
    WHERE voltage BETWEEN 216.1 AND 234.7
),
summary AS (
    -- Group-level totals
    SELECT
        site_id,
        site_tariff,
        COUNT(*) AS n_rows,
        ROUND(100 * COUNT(CASE WHEN site_tariff = 'EV' THEN 1 END) / COUNT(*), 1) AS pct_site_tariff_ev,
        ROUND(SUM(quality_len) / NULLIF(SUM(read_at_dayofweek), 0), 3) AS quality_len_to_read_at_dayofweek_ratio
    FROM enriched
    GROUP BY site_id, site_tariff
),
labeled AS (
    -- Derive helper columns
    SELECT
        site_id,
        site_tariff,
        n_rows,
        pct_site_tariff_ev,
        quality_len_to_read_at_dayofweek_ratio,
        COALESCE(site_tariff, 'UNKNOWN') AS site_tariff_or_unknown
    FROM summary
),
filtered AS (
    -- Rows that have matching meter_readings
    SELECT s.*
    FROM labeled s
    WHERE EXISTS (
            SELECT 1
            FROM meter_readings m
            WHERE m.site_id = s.site_id
              AND m.kwh BETWEEN 42.1 AND 109.3
        )
)
SELECT
    site_id,
    n_rows,
    pct_site_tariff_ev,
    quality_len_to_read_at_dayofweek_ratio,
    SUBSTRING(TRIM(site_tariff), 1, 2) AS site_tariff_prefix
FROM filtered
ORDER BY site_id
