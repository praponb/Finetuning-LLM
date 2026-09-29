WITH valid_readings AS (
    -- Scope the meter_readings rows
    SELECT
        id,
        site_id,
        kwh,
        voltage,
        quality,
        MONTH(read_at) AS read_at_month,
        FLOOR(voltage / 5) * 5 AS voltage_bucket,
        SUBSTRING(TRIM(quality), 1, 4) AS quality_prefix
    FROM meter_readings
    WHERE site_id IS NOT NULL
),
screened AS (
    -- Keep rows whose site matches
    SELECT s.*
    FROM valid_readings s
    WHERE EXISTS (
            SELECT 1
            FROM sites si
            WHERE si.id = s.site_id
              AND si.zone = 'Z4'
              AND si.tariff IN ('TOU', 'FLAT')
        )
),
rollup_base AS (
    -- Summarise per group
    SELECT
        quality,
        site_id,
        SUM(kwh) AS total_kwh,
        ARRAY_SORT(COLLECT_SET(quality)) AS quality_list
    FROM screened
    WHERE quality_prefix IS NOT NULL
    GROUP BY 1, 2
    HAVING SUM(read_at_month) > 2
),
windowed AS (
    -- Previous / next values per key
    SELECT
        quality,
        site_id,
        total_kwh,
        quality_list,
        ROUND(total_kwh - AVG(total_kwh) OVER (PARTITION BY site_id, quality), 2) AS total_kwh_vs_site_id_quality_avg,
        MIN(total_kwh) OVER (PARTITION BY site_id, quality) AS site_id_quality_min_total_kwh,
        DENSE_RANK() OVER (PARTITION BY site_id, quality ORDER BY total_kwh ASC) AS total_kwh_dense_rank,
        ROUND(CUME_DIST() OVER (PARTITION BY site_id, quality ORDER BY total_kwh ASC), 3) AS total_kwh_cume_dist
    FROM rollup_base
    WINDOW w_site_id_quality AS (PARTITION BY site_id, quality)
)
-- Final projection
SELECT
    quality,
    total_kwh,
    quality_list,
    site_id_quality_min_total_kwh,
    total_kwh_dense_rank,
    total_kwh_cume_dist
FROM windowed
ORDER BY total_kwh DESC, total_kwh_dense_rank NULLS LAST
