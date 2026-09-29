WITH recent AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        site_id,
        read_at,
        kwh,
        voltage,
        quality
    FROM meter_readings
    WHERE quality IN ('EST', 'OK')
      AND site_id IS NOT NULL
),
with_children AS (
    -- One row per reading
    SELECT
        s.id,
        s.site_id,
        s.read_at,
        s.kwh,
        s.quality,
        m.id AS reading_id,
        m.kwh AS reading_kwh,
        m.read_at AS reading_read_at
    FROM recent s
    LEFT JOIN meter_readings m ON m.site_id = s.site_id AND m.read_at < TIMESTAMP '2025-02-24 10:00:00'
),
labeled AS (
    -- Add calculated columns
    SELECT
        *,
        COALESCE(quality, 'UNKNOWN') AS quality_or_unknown,
        DATEDIFF(DATE '2025-06-30', read_at) AS days_since_read_at,
        COALESCE(kwh, 0.0) AS kwh_filled
    FROM with_children
    WHERE read_at < TIMESTAMP '2025-02-18 09:00:00'
),
enriched AS (
    -- Attach site attributes
    SELECT
        s.id,
        s.site_id,
        s.read_at,
        s.kwh,
        s.quality,
        s.reading_id,
        s.reading_kwh,
        s.quality_or_unknown,
        s.days_since_read_at,
        s.kwh_filled,
        si.site_name
    FROM labeled s
    JOIN sites si ON si.id = s.site_id
    WHERE UPPER(TRIM(si.site_name)) LIKE 'D%'
),
ranked AS (
    -- Running and moving aggregates
    SELECT
        *,
        LEAD(read_at, 3) OVER (PARTITION BY reading_id ORDER BY kwh, id, reading_id) AS next3_read_at,
        ROUND(kwh_filled - AVG(kwh_filled) OVER (PARTITION BY site_id, quality), 2) AS kwh_filled_vs_site_id_quality_avg,
        ROUND(kwh_filled / NULLIF(SUM(kwh_filled) OVER w_site_id_quality, 0), 4) AS kwh_filled_share_of_site_id_quality,
        SIZE(COLLECT_SET(reading_id) OVER (PARTITION BY site_id, quality)) AS distinct_reading_id_in_site_id_quality
    FROM enriched
    WINDOW w_site_id_quality AS (PARTITION BY site_id, quality)
)
-- Native SQL UNPIVOT (wide to long)
SELECT id, reading_id, field, amount
FROM (
    SELECT id, reading_id, reading_kwh, kwh_filled
    FROM ranked
) src
UNPIVOT (
    amount FOR field IN (reading_kwh AS reading_kwh_v, kwh_filled AS kwh_filled_v)
) unp
ORDER BY id, reading_id, field
