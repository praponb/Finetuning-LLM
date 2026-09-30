WITH filtered AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        site_id,
        read_at,
        kwh,
        voltage,
        quality
    FROM meter_readings
    WHERE (HOUR(read_at) BETWEEN 6 AND 9 OR quality = 'EST')
),
enriched AS (
    -- Label and score rows
    SELECT
        *,
        SUBSTRING(TRIM(quality), 1, 4) AS quality_prefix,
        COALESCE(kwh, 0.0) AS kwh_filled,
        UPPER(TRIM(quality)) AS quality_clean
    FROM filtered
),
with_windows AS (
    -- Rank and compare rows within groups
    SELECT
        id,
        site_id,
        read_at,
        quality,
        quality_prefix,
        quality_clean,
        ROUND(CUME_DIST() OVER (PARTITION BY quality, site_id ORDER BY voltage DESC, id), 3) AS voltage_cume_dist,
        LAG(kwh_filled, 3) OVER (ORDER BY kwh_filled ASC, id) AS prev3_kwh_filled,
        NTILE(4) OVER (ORDER BY kwh_filled ASC, id) AS kwh_filled_quartile,
        NTH_VALUE(read_at, 2) OVER (ORDER BY kwh_filled ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_read_at,
        SIZE(COLLECT_SET(quality) OVER (PARTITION BY site_id)) AS distinct_quality_in_site_id
    FROM enriched
)
-- Native SQL UNPIVOT (wide to long)
SELECT id, measure, value
FROM (
    SELECT id, voltage_cume_dist, prev3_kwh_filled
    FROM with_windows
) src
UNPIVOT (
    value FOR measure IN (voltage_cume_dist, prev3_kwh_filled)
) unp
