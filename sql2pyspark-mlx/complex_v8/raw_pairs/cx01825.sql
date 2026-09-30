WITH recent AS (
    -- Filter meter_readings
    SELECT
        id,
        site_id,
        read_at,
        kwh,
        voltage,
        quality
    FROM meter_readings
    WHERE (kwh <= 110.5 OR MONTH(read_at) IN (2, 3))
),
ranked AS (
    -- Previous / next values per key
    SELECT
        *,
        ROUND(CUME_DIST() OVER (PARTITION BY site_id ORDER BY read_at, id), 3) AS read_at_cume_dist,
        FIRST_VALUE(read_at) IGNORE NULLS OVER (ORDER BY voltage NULLS LAST, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_read_at,
        RANK() OVER (PARTITION BY site_id ORDER BY read_at, id) AS read_at_rank
    FROM recent
),
labeled AS (
    -- Derive helper columns
    SELECT
        *,
        MONTH(first_known_read_at) AS first_known_read_at_month,
        LENGTH(TRIM(quality)) AS quality_len,
        LENGTH(TRIM(quality)) AS quality_len_2
    FROM ranked
    WHERE read_at_cume_dist >= 0.1
),
with_details AS (
    -- Attach site attributes
    SELECT
        s.id,
        s.site_id,
        s.read_at,
        s.kwh,
        s.voltage,
        s.read_at_cume_dist,
        s.first_known_read_at,
        s.read_at_rank,
        s.quality_len,
        si.installed_kw AS site_installed_kw,
        si.commissioned_on AS site_commissioned_on,
        si.zone AS site_zone
    FROM labeled s
    JOIN sites si ON si.id = s.site_id
),
ranked_2 AS (
    -- Window metrics over several partitions
    SELECT
        id,
        read_at,
        voltage,
        read_at_cume_dist,
        first_known_read_at,
        quality_len,
        site_installed_kw,
        site_commissioned_on,
        SUM(read_at_rank) OVER (PARTITION BY quality_len ORDER BY read_at ASC, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS read_at_rank_sum_7,
        MAX(site_installed_kw) OVER (PARTITION BY site_id, site_zone) AS site_id_site_zone_max_site_installed_kw
    FROM with_details
),
summary AS (
    -- Group-level totals
    SELECT
        DATE_TRUNC('MONTH', first_known_read_at) AS first_known_read_at_month,
        quality_len,
        DATE_FORMAT(site_commissioned_on, 'yyyy-MM') AS site_commissioned_on_ym,
        MAX(read_at_cume_dist) AS max_read_at_cume_dist,
        ROUND(SUM(site_id_site_zone_max_site_installed_kw) / NULLIF(SUM(voltage), 0), 3) AS site_id_site_zone_max_site_installed_kw_to_volta,
        COUNT(DISTINCT id) AS distinct_ids,
        PERCENTILE_APPROX(site_installed_kw, 0.5) AS median_site_installed_kw,
        MAX(read_at_rank_sum_7) AS max_read_at_rank_sum_7
    FROM ranked_2
    GROUP BY DATE_TRUNC('MONTH', first_known_read_at), quality_len, DATE_FORMAT(site_commissioned_on, 'yyyy-MM')
    HAVING MAX(site_installed_kw) > 141.6
)
-- Final projection
SELECT
    first_known_read_at_month,
    quality_len,
    site_commissioned_on_ym,
    max_read_at_cume_dist,
    site_id_site_zone_max_site_installed_kw_to_volta,
    max_read_at_rank_sum_7
FROM summary
ORDER BY max_read_at_cume_dist DESC, site_commissioned_on_ym, first_known_read_at_month, quality_len
LIMIT 10
