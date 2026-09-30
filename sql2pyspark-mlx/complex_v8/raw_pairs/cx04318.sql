WITH cleaned_readings AS (
    -- Scope the meter_readings rows
    SELECT
        id,
        site_id,
        read_at,
        kwh,
        voltage,
        quality,
        COALESCE(kwh, 0.0) AS kwh_filled
    FROM meter_readings
    WHERE site_id IS NOT NULL
),
with_parent_site AS (
    -- Attach site attributes
    SELECT
        s.*,
        si.commissioned_on AS site_commissioned_on,
        si.installed_kw AS site_installed_kw,
        si.zone AS site_zone
    FROM cleaned_readings s
    LEFT JOIN sites si ON si.id = s.site_id
),
metrics AS (
    -- Running and moving aggregates
    SELECT
        *,
        RANK() OVER (PARTITION BY site_zone, quality ORDER BY kwh_filled DESC) AS kwh_filled_rank,
        ROUND(PERCENT_RANK() OVER (PARTITION BY YEAR(site_commissioned_on), site_zone ORDER BY voltage DESC), 3) AS voltage_percent_rank,
        ROUND(site_installed_kw / NULLIF(SUM(site_installed_kw) OVER (PARTITION BY site_id, quality), 0), 4) AS site_installed_kw_share_of_site_id_quality,
        FIRST_VALUE(site_installed_kw) OVER (PARTITION BY site_zone, quality ORDER BY kwh_filled DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_site_installed_kw,
        MAX(site_installed_kw) OVER (PARTITION BY site_zone, quality ORDER BY kwh_filled DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS max_site_installed_kw_so_far,
        ROUND(AVG(kwh_filled) OVER (PARTITION BY site_zone, quality ORDER BY kwh_filled DESC, id ROWS BETWEEN 1 PRECEDING AND CURRENT ROW), 2) AS kwh_filled_avg_2,
        ROW_NUMBER() OVER (PARTITION BY site_zone, quality ORDER BY kwh_filled DESC, id) AS kwh_filled_seq,
        FIRST_VALUE(voltage) OVER (PARTITION BY quality, site_zone ORDER BY kwh, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_voltage,
        MAX(kwh) OVER (PARTITION BY site_id, quality) AS site_id_quality_max_kwh,
        site_installed_kw - LAG(site_installed_kw) OVER (PARTITION BY YEAR(site_commissioned_on), site_zone ORDER BY voltage DESC, id) AS site_installed_kw_change,
        MAX(kwh_filled) OVER w_site_id_quality AS site_id_quality_max_kwh_filled,
        LAG(kwh) OVER (PARTITION BY quality, site_zone ORDER BY kwh, id) AS prev_kwh,
        SUM(voltage) OVER (PARTITION BY site_id, quality) AS site_id_quality_sum_voltage,
        LAST_VALUE(read_at) IGNORE NULLS OVER (PARTITION BY site_zone, quality ORDER BY kwh_filled DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_read_at,
        LAG(read_at) OVER (PARTITION BY YEAR(site_commissioned_on), site_zone ORDER BY voltage DESC, id) AS prev_read_at
    FROM with_parent_site
    WINDOW w_site_id_quality AS (PARTITION BY site_id, quality)
),
windowed AS (
    -- Running and moving aggregates
    SELECT
        id,
        site_id,
        read_at,
        kwh,
        quality,
        site_commissioned_on,
        site_installed_kw,
        site_zone,
        kwh_filled_rank,
        voltage_percent_rank,
        site_installed_kw_share_of_site_id_quality,
        kwh_filled_avg_2,
        kwh_filled_seq,
        first_voltage,
        site_installed_kw_change,
        site_id_quality_max_kwh_filled,
        prev_kwh,
        site_id_quality_sum_voltage,
        last_known_read_at,
        ROUND(CUME_DIST() OVER (ORDER BY last_known_read_at DESC), 3) AS last_known_read_at_cume_dist,
        NTH_VALUE(site_id_quality_max_kwh, 2) OVER (ORDER BY last_known_read_at DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_site_id_quality_max_kwh,
        ROUND(first_site_installed_kw / NULLIF(SUM(first_site_installed_kw) OVER (PARTITION BY DATE_TRUNC('MONTH', last_known_read_at)), 0), 4) AS first_site_installed_kw_share_of_last_known_read,
        MAX(site_installed_kw) OVER (ORDER BY last_known_read_at DESC, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS site_installed_kw_max_4,
        LAG(read_at) OVER (PARTITION BY site_zone, kwh_filled_rank ORDER BY site_id_quality_max_kwh ASC, id) AS prev_read_at_2
    FROM metrics
)
SELECT
    id,
    read_at,
    kwh,
    site_commissioned_on,
    site_zone,
    kwh_filled_rank,
    kwh_filled_avg_2,
    kwh_filled_seq,
    first_voltage,
    site_installed_kw_change,
    last_known_read_at,
    last_known_read_at_cume_dist,
    second_site_id_quality_max_kwh,
    first_site_installed_kw_share_of_last_known_read,
    prev_read_at_2,
    FLOOR(prev_kwh / 25) * 25 AS prev_kwh_bucket
FROM windowed
WHERE kwh_filled_avg_2 <= 72.0
