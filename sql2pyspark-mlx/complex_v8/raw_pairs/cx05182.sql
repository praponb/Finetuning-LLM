WITH filtered AS (
    -- Filter meter_readings
    SELECT
        id,
        site_id,
        kwh,
        voltage,
        quality
    FROM meter_readings
    WHERE voltage > 219.6
),
enriched AS (
    -- Enrich with dimension data
    SELECT
        s.*,
        si.commissioned_on AS site_commissioned_on,
        si.installed_kw AS site_installed_kw
    FROM filtered s
    LEFT JOIN sites si ON si.id = s.site_id
),
with_windows AS (
    -- Window metrics over several partitions
    SELECT
        *,
        ROUND(PERCENT_RANK() OVER (ORDER BY site_installed_kw ASC, id), 3) AS site_installed_kw_percent_rank,
        ROUND(AVG(kwh) OVER (ORDER BY site_installed_kw ASC, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW), 2) AS kwh_avg_4,
        ROUND(AVG(site_installed_kw) OVER (PARTITION BY quality ORDER BY site_commissioned_on DESC, id ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING), 2) AS site_installed_kw_avg_6_prior,
        ROUND(AVG(voltage) OVER (ORDER BY site_installed_kw ASC, id ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING), 2) AS voltage_avg_6_prior
    FROM enriched
),
with_stats AS (
    -- Attach meter_readings metrics
    SELECT
        s.*,
        (SELECT COUNT(*) FROM meter_readings m WHERE m.site_id = s.site_id) AS reading_count,
        (SELECT MAX(m.read_at) FROM meter_readings m WHERE m.site_id = s.site_id) AS last_reading_read_at,
        (SELECT COUNT(*) FROM meter_readings m WHERE m.site_id = s.site_id) AS reading_count_2
    FROM with_windows s
),
metrics AS (
    -- Previous / next values per key
    SELECT
        *,
        DENSE_RANK() OVER w_reading_count_site_id_by_sit AS site_commissioned_on_dense_rank,
        COUNT(*) OVER (PARTITION BY reading_count_2) AS rows_in_reading_count_2,
        LAG(kwh_avg_4) OVER w_reading_count_site_id_by_sit_2 AS prev_kwh_avg_4,
        ROUND(reading_count / NULLIF(SUM(reading_count) OVER w_reading_count_2, 0), 4) AS reading_count_share_of_reading_count_2,
        ROUND(CUME_DIST() OVER (PARTITION BY reading_count_2, quality ORDER BY reading_count), 3) AS reading_count_cume_dist
    FROM with_stats
    WINDOW w_reading_count_site_id_by_sit AS (PARTITION BY reading_count, site_id ORDER BY site_commissioned_on), w_reading_count_site_id_by_sit_2 AS (PARTITION BY reading_count, site_id ORDER BY site_commissioned_on, id), w_reading_count_2 AS (PARTITION BY reading_count_2)
),
screened AS (
    -- Keep rows whose site matches
    SELECT s.*
    FROM metrics s
    WHERE EXISTS (
            SELECT 1
            FROM sites si
            WHERE si.id = s.site_id
              AND si.tariff NOT IN ('EV', 'TOU')
              AND si.installed_kw <= 373.3
        )
),
reading_stats_2 AS (
    -- Per-site meter_readings metrics
    SELECT
        site_id,
        COUNT(*) FILTER (WHERE quality = 'OK') AS reading_quality_count,
        MAX(read_at) AS last_reading_read_at_2,
        COUNT(*) AS reading_count_3
    FROM meter_readings
    GROUP BY site_id
),
with_stats_2 AS (
    -- Attach meter_readings metrics
    SELECT
        s.*,
        COALESCE(m.reading_quality_count, 0) AS reading_quality_count,
        m.last_reading_read_at_2,
        COALESCE(m.reading_count_3, 0) AS reading_count_3
    FROM screened s
    LEFT JOIN reading_stats_2 m ON m.site_id = s.site_id
)
SELECT
    id,
    kwh,
    voltage,
    quality,
    site_installed_kw,
    site_installed_kw_percent_rank,
    kwh_avg_4,
    site_installed_kw_avg_6_prior,
    reading_count,
    last_reading_read_at,
    site_commissioned_on_dense_rank,
    prev_kwh_avg_4,
    reading_count_cume_dist,
    reading_quality_count,
    last_reading_read_at_2,
    reading_count_3,
    YEAR(site_commissioned_on) AS site_commissioned_on_year,
    TO_DATE(last_reading_read_at) AS last_reading_read_at_date
FROM with_stats_2
WHERE quality NOT IN ('OK', 'EST')
ORDER BY site_installed_kw_avg_6_prior, quality
