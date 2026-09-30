WITH cleaned AS (
    -- Scope the meter_readings rows
    SELECT
        id,
        site_id,
        read_at,
        kwh,
        voltage,
        quality,
        DATE_FORMAT(read_at, 'yyyy-MM') AS read_at_label,
        DATE_FORMAT(read_at, 'yyyy-MM') AS read_at_label_2,
        CASE WHEN quality IN ('OK', 'EST') THEN 'group_a' WHEN quality = 'BAD' THEN 'group_b' ELSE 'other' END AS quality_group
    FROM meter_readings
),
quality_group_stats AS (
    -- Median kwh per quality_group
    SELECT quality_group, PERCENTILE_APPROX(kwh, 0.5) AS median_kwh
    FROM cleaned
    GROUP BY quality_group
),
vs_group AS (
    -- Rows compared with their group statistic
    SELECT s.*, g.median_kwh, ROUND(s.kwh - g.median_kwh, 2) AS kwh_minus_median
    FROM cleaned s
    JOIN quality_group_stats g ON g.quality_group = s.quality_group
    WHERE s.kwh >= g.median_kwh
),
filtered AS (
    -- Rows that have matching meter_readings
    SELECT s.*
    FROM vs_group s
    WHERE EXISTS (
            SELECT 1
            FROM meter_readings m
            WHERE m.site_id = s.site_id
              AND m.voltage < 231.6
        )
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
        s.read_at_label,
        s.read_at_label_2,
        s.quality_group,
        s.median_kwh,
        s.kwh_minus_median,
        si.zone AS site_zone,
        si.tariff AS site_tariff,
        si.commissioned_on AS site_commissioned_on
    FROM filtered s
    JOIN sites si ON si.id = s.site_id
    WHERE si.commissioned_on < DATE '2025-05-10'
),
enriched_site_2 AS (
    -- Join sites
    SELECT
        s.id,
        s.site_id,
        s.read_at,
        s.kwh,
        s.quality,
        s.read_at_label,
        s.read_at_label_2,
        s.quality_group,
        s.median_kwh,
        s.kwh_minus_median,
        s.site_zone,
        s.site_tariff,
        s.site_commissioned_on,
        si.site_name,
        si.zone AS site_zone_2
    FROM enriched_site s
    JOIN sites si ON si.id = s.site_id
),
enriched AS (
    -- Derive helper columns
    SELECT
        id,
        read_at,
        kwh,
        quality,
        read_at_label_2,
        quality_group,
        median_kwh,
        kwh_minus_median,
        site_commissioned_on,
        site_name,
        YEAR(read_at) AS read_at_year,
        COALESCE(kwh, 0.0) AS kwh_filled
    FROM enriched_site_2
    WHERE site_tariff IN ('TOU', 'EV')
),
with_windows AS (
    -- Running and moving aggregates
    SELECT
        id,
        read_at,
        kwh,
        quality,
        quality_group,
        median_kwh,
        kwh_minus_median,
        site_commissioned_on,
        site_name,
        MIN(read_at_year) OVER w_read_at_year_quality AS read_at_year_quality_min_read_at_year,
        NTH_VALUE(read_at, 2) OVER (PARTITION BY read_at_year ORDER BY median_kwh DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_read_at,
        ROW_NUMBER() OVER (PARTITION BY read_at_year ORDER BY median_kwh DESC, id) AS median_kwh_seq
    FROM enriched
    WINDOW w_read_at_year_quality AS (PARTITION BY read_at_year, quality)
)
SELECT DISTINCT quality
FROM with_windows
ORDER BY quality DESC NULLS FIRST
