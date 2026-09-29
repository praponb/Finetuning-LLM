WITH matched AS (
    -- Price each reading with the tariff window it falls into
    SELECT
        r.id,
        r.site_id,
        r.read_at,
        r.kwh,
        r.quality,
        st.zone,
        tw.rate,
        ROUND(r.kwh * tw.rate, 2) AS energy_cost
    FROM meter_readings r
    JOIN sites st ON st.id = r.site_id
    JOIN tariff_windows tw
      ON tw.tariff = st.tariff
     AND r.read_at >= tw.valid_from
     AND r.read_at < tw.valid_to
),
per_group AS (
    -- Aggregate with conditional metrics
    SELECT
        site_id,
        zone,
        COUNT(*) AS n_rows,
        ROUND(SUM(rate), 2) AS total_rate,
        MAX(read_at) AS last_read_at,
        COUNT(*) FILTER (WHERE quality IN ('BAD', 'OK')) AS n_quality_in_bad_ok,
        ROUND(SUM(energy_cost) / NULLIF(SUM(kwh), 0), 3) AS energy_cost_to_kwh_ratio,
        ROUND(100 * COUNT(CASE WHEN quality <> 'BAD' THEN 1 END) / COUNT(*), 1) AS pct_quality_bad
    FROM matched
    GROUP BY site_id, zone
),
filtered AS (
    -- Rows with no matching meter_readings
    SELECT s.*
    FROM per_group s
    WHERE NOT EXISTS (
            SELECT 1
            FROM meter_readings m
            WHERE m.site_id = s.site_id
              AND m.voltage BETWEEN 213.7 AND 238.8
              AND m.quality NOT IN ('EST', 'OK')
        )
),
enriched AS (
    -- Join sites
    SELECT
        s.site_id,
        s.zone,
        s.total_rate,
        s.last_read_at,
        s.n_quality_in_bad_ok,
        s.pct_quality_bad,
        si.commissioned_on AS site_commissioned_on,
        si.zone AS site_zone,
        si.tariff AS site_tariff
    FROM filtered s
    JOIN sites si ON si.id = s.site_id
)
SELECT
    site_id,
    total_rate,
    last_read_at,
    n_quality_in_bad_ok,
    pct_quality_bad,
    site_commissioned_on,
    site_zone,
    site_tariff,
    LOWER(REGEXP_REPLACE(TRIM(zone), ' ', '_')) AS zone_slug,
    DATE_TRUNC('YEAR', site_commissioned_on) AS site_commissioned_on_year
FROM enriched
ORDER BY total_rate
