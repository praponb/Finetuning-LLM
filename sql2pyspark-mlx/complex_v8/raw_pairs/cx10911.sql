WITH base AS (
    -- Scope the meter_readings rows
    SELECT
        id,
        site_id,
        read_at,
        kwh,
        quality,
        SUBSTRING(TRIM(quality), 1, 3) AS quality_prefix
    FROM meter_readings
    WHERE kwh BETWEEN 37.1 AND 85.7
      AND HOUR(read_at) BETWEEN 6 AND 8
),
joined AS (
    -- Join sites
    SELECT
        s.id,
        s.site_id,
        s.read_at,
        s.kwh,
        s.quality_prefix,
        si.commissioned_on AS site_commissioned_on
    FROM base s
    LEFT JOIN sites si ON si.id = s.site_id
),
enriched AS (
    -- Label and score rows
    SELECT id, DATE_ADD(site_commissioned_on, 7) AS site_commissioned_on_plus_7d
    FROM joined
),
sequenced AS (
    -- Window metrics over several partitions
    SELECT
        *,
        RANK() OVER w_all_by_site_commissioned_on AS site_commissioned_on_plus_7d_rank,
        NTILE(5) OVER (ORDER BY site_commissioned_on_plus_7d ASC NULLS LAST, id) AS site_commissioned_on_plus_7d_quintile,
        ROUND(CUME_DIST() OVER (ORDER BY site_commissioned_on_plus_7d DESC), 3) AS site_commissioned_on_plus_7d_cume_dist
    FROM enriched
    WINDOW w_all_by_site_commissioned_on AS (ORDER BY site_commissioned_on_plus_7d DESC)
)
SELECT
    site_commissioned_on_plus_7d_quintile,
    site_commissioned_on_plus_7d_rank,
    COUNT(DISTINCT id) AS distinct_ids,
    ROUND(STDDEV(site_commissioned_on_plus_7d_cume_dist), 2) AS stddev_site_commissioned_on_plus_7d_cume_dist
FROM sequenced
GROUP BY GROUPING SETS ((site_commissioned_on_plus_7d_quintile, site_commissioned_on_plus_7d_rank), (site_commissioned_on_plus_7d_quintile))
ORDER BY site_commissioned_on_plus_7d_quintile NULLS LAST, site_commissioned_on_plus_7d_rank NULLS LAST
