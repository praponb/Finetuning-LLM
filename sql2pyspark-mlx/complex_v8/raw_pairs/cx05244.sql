WITH recent AS (
    -- Scope the sites rows
    SELECT
        id,
        site_name,
        zone,
        tariff,
        commissioned_on
    FROM sites
    WHERE (tariff IN ('TOU', 'EV') OR YEAR(commissioned_on) = 2024)
),
with_children AS (
    -- One row per reading
    SELECT
        s.id,
        s.site_name,
        s.tariff,
        m.id AS reading_id,
        m.read_at AS reading_read_at,
        m.quality AS reading_quality
    FROM recent s
    JOIN meter_readings m ON m.site_id = s.id
),
qualified AS (
    -- Rows that have matching meter_readings
    SELECT s.*
    FROM with_children s
    WHERE EXISTS (
            SELECT 1
            FROM meter_readings m
            WHERE m.site_id = s.id
        )
      AND s.reading_id IS NOT NULL
),
scored AS (
    -- Label and score rows
    SELECT
        *,
        YEAR(reading_read_at) AS reading_read_at_year,
        LOWER(REGEXP_REPLACE(TRIM(site_name), ' ', '_')) AS site_name_slug
    FROM qualified
),
with_windows AS (
    -- Previous / next values per key
    SELECT
        *,
        COUNT(*) OVER (PARTITION BY reading_id) AS rows_in_reading_id,
        ROUND(AVG(reading_read_at_year) OVER (PARTITION BY reading_id, tariff ORDER BY reading_read_at, id, reading_id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW), 2) AS reading_read_at_year_avg_4,
        SUM(reading_read_at_year) OVER (ORDER BY reading_read_at_year, id, reading_id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_reading_read_at_year,
        ROUND(PERCENT_RANK() OVER (ORDER BY reading_read_at_year DESC NULLS FIRST, id, reading_id), 3) AS reading_read_at_year_percent_rank
    FROM scored
),
ranked AS (
    -- Previous / next values per key
    SELECT
        *,
        MAX(reading_read_at_year) OVER (PARTITION BY reading_quality, reading_read_at_year) AS reading_quality_reading_read_at_year_max_reading,
        ROUND(reading_read_at_year - AVG(reading_read_at_year) OVER (PARTITION BY reading_quality, reading_read_at_year), 2) AS reading_read_at_year_vs_reading_quality_reading_,
        ROUND(AVG(reading_read_at_year_avg_4) OVER (ORDER BY reading_read_at_year, id, reading_id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW), 2) AS reading_read_at_year_avg_4_avg_7,
        MAX(rows_in_reading_id) OVER (ORDER BY reading_read_at_year, id, reading_id ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING) AS rows_in_reading_id_max_2_prior,
        RANK() OVER (ORDER BY reading_read_at_year) AS reading_read_at_year_rank
    FROM with_windows
)
-- Native SQL PIVOT (long to wide)
SELECT *
FROM (
    SELECT reading_id, tariff, reading_quality, reading_read_at_year
    FROM ranked
) src
PIVOT (
    MIN(reading_read_at_year)
    FOR reading_quality IN ('BAD', 'OK', 'EST')
)
ORDER BY reading_id, tariff
