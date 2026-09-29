WITH scoped AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        site_id,
        read_at,
        kwh,
        quality
    FROM meter_readings
    WHERE voltage <= 238.8
      AND kwh >= 20.4
),
sequenced AS (
    -- Window metrics over several partitions
    SELECT
        *,
        NTILE(5) OVER (PARTITION BY site_id, quality ORDER BY read_at ASC, id) AS read_at_quintile,
        ROW_NUMBER() OVER (PARTITION BY site_id, quality ORDER BY read_at ASC, id) AS read_at_seq,
        ROUND(CUME_DIST() OVER (PARTITION BY site_id, quality ORDER BY read_at ASC), 3) AS read_at_cume_dist,
        NTILE(3) OVER (PARTITION BY site_id, quality ORDER BY read_at ASC, id) AS read_at_tercile
    FROM scoped
),
enriched_site AS (
    -- Attach site attributes
    SELECT
        s.id,
        s.site_id,
        s.read_at,
        s.kwh,
        s.quality,
        s.read_at_quintile,
        s.read_at_cume_dist,
        si.tariff AS site_tariff,
        si.installed_kw AS site_installed_kw
    FROM sequenced s
    LEFT JOIN sites si ON si.id = s.site_id
),
reading_stats AS (
    -- Per-site meter_readings metrics
    SELECT site_id, MAX(read_at) AS last_reading_read_at
    FROM meter_readings
    GROUP BY site_id
),
summarized_rows AS (
    -- Attach meter_readings metrics
    SELECT
        s.*,
        m.last_reading_read_at
    FROM enriched_site s
    LEFT JOIN reading_stats m ON m.site_id = s.site_id
),
enriched_site_2 AS (
    -- Join sites
    SELECT
        s.id,
        s.site_id,
        s.read_at,
        s.quality,
        s.read_at_cume_dist,
        s.site_tariff,
        s.site_installed_kw,
        s.last_reading_read_at,
        si.tariff AS site_tariff_2
    FROM summarized_rows s
    LEFT JOIN sites si ON si.id = s.site_id
),
with_details AS (
    -- Attach site attributes
    SELECT
        s.id,
        s.site_id,
        s.read_at,
        s.read_at_cume_dist,
        s.site_tariff,
        s.site_installed_kw,
        s.last_reading_read_at,
        s.site_tariff_2,
        si.site_name
    FROM enriched_site_2 s
    JOIN sites si ON si.id = s.site_id
    WHERE UPPER(TRIM(si.site_name)) LIKE 'R%'
),
metrics AS (
    -- Running and moving aggregates
    SELECT
        *,
        ROUND(AVG(site_installed_kw) OVER (PARTITION BY site_id, site_tariff), 2) AS site_id_site_tariff_avg_site_installed_kw,
        LAG(read_at_cume_dist, 1, 0) OVER (ORDER BY read_at_cume_dist ASC NULLS LAST, id) AS prev_read_at_cume_dist,
        COUNT(*) OVER (PARTITION BY site_id, site_tariff) AS rows_in_site_id_site_tariff,
        ROUND(site_installed_kw / NULLIF(SUM(site_installed_kw) OVER (PARTITION BY site_id, site_tariff), 0), 4) AS site_installed_kw_share_of_site_id_site_tariff
    FROM with_details
)
SELECT
    COALESCE(site_tariff_2, 'ALL') AS site_tariff_2,
    site_id,
    COALESCE(site_tariff, 'ALL') AS site_tariff,
    GROUPING(site_tariff_2) AS is_total_site_tariff_2,
    COUNT(DISTINCT id) AS distinct_ids,
    SUM(CASE WHEN UPPER(TRIM(site_name)) LIKE 'H%' THEN 1 ELSE 0 END) AS n_upper_trim_site_name_like_h,
    ROUND(SUM(site_installed_kw_share_of_site_id_site_tariff) / NULLIF(SUM(rows_in_site_id_site_tariff), 0), 3) AS site_installed_kw_share_of_site_id_site_tariff_t
FROM metrics
GROUP BY GROUPING SETS ((site_tariff_2, site_id, site_tariff), (site_tariff_2))
ORDER BY site_tariff_2 NULLS LAST, site_id NULLS LAST, site_tariff NULLS LAST
