WITH scoped_readings AS (
    -- Filter meter_readings
    SELECT
        id,
        site_id,
        read_at,
        kwh,
        voltage,
        quality,
        LOWER(REGEXP_REPLACE(TRIM(quality), ' ', '_')) AS quality_slug
    FROM meter_readings
    WHERE voltage < 236.8
),
with_details_site AS (
    -- Join sites
    SELECT
        s.id,
        s.site_id,
        s.read_at,
        s.kwh,
        s.voltage,
        s.quality,
        s.quality_slug,
        si.zone AS site_zone
    FROM scoped_readings s
    LEFT JOIN sites si ON si.id = s.site_id
),
with_trends AS (
    -- Previous / next values per key
    SELECT
        id,
        read_at,
        voltage,
        quality,
        quality_slug,
        site_zone,
        SUM(kwh) OVER (PARTITION BY quality, site_zone) AS quality_site_zone_sum_kwh,
        ROUND(CUME_DIST() OVER (PARTITION BY TO_DATE(read_at) ORDER BY read_at, id), 3) AS read_at_cume_dist,
        MAX(voltage) OVER (PARTITION BY TO_DATE(read_at) ORDER BY read_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS max_voltage_so_far,
        NTH_VALUE(voltage, 2) OVER (PARTITION BY TO_DATE(read_at) ORDER BY read_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_voltage,
        ROUND(PERCENT_RANK() OVER (PARTITION BY TO_DATE(read_at) ORDER BY read_at, id), 3) AS read_at_percent_rank,
        COUNT(*) OVER (PARTITION BY TO_DATE(read_at) ORDER BY read_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_count,
        NTILE(10) OVER (PARTITION BY quality, site_zone ORDER BY kwh, id) AS kwh_decile,
        ROUND(AVG(voltage) OVER w_quality_site_zone, 2) AS quality_site_zone_avg_voltage,
        DENSE_RANK() OVER (PARTITION BY site_zone, site_id ORDER BY kwh DESC NULLS FIRST) AS kwh_dense_rank,
        RANK() OVER w_site_zone_site_id_by_kwh_tie AS kwh_rank,
        ROW_NUMBER() OVER w_quality_site_zone_by_kwh AS kwh_seq,
        LAST_VALUE(read_at) IGNORE NULLS OVER (PARTITION BY quality, site_zone ORDER BY kwh, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_read_at,
        NTH_VALUE(voltage, 2) OVER (PARTITION BY quality, site_zone ORDER BY kwh, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_voltage_2,
        ROUND(PERCENT_RANK() OVER w_quality_site_zone_by_kwh_tie, 3) AS kwh_percent_rank
    FROM with_details_site
    WINDOW w_quality_site_zone AS (PARTITION BY quality, site_zone), w_site_zone_site_id_by_kwh_tie AS (PARTITION BY site_zone, site_id ORDER BY kwh DESC NULLS FIRST), w_quality_site_zone_by_kwh AS (PARTITION BY quality, site_zone ORDER BY kwh, id), w_quality_site_zone_by_kwh_tie AS (PARTITION BY quality, site_zone ORDER BY kwh)
),
leaders AS (
    -- First row per group
    SELECT
        id,
        read_at,
        voltage,
        quality,
        quality_slug,
        site_zone,
        quality_site_zone_sum_kwh,
        read_at_cume_dist,
        max_voltage_so_far,
        second_voltage,
        read_at_percent_rank,
        running_count,
        kwh_decile,
        quality_site_zone_avg_voltage,
        kwh_rank,
        kwh_seq,
        last_known_read_at,
        second_voltage_2,
        kwh_percent_rank
    FROM with_trends
    WHERE kwh_dense_rank = 1
      AND site_zone IS NOT NULL
),
ranked AS (
    -- Previous / next values per key
    SELECT
        *,
        LEAD(max_voltage_so_far, 1, 0) OVER (ORDER BY kwh_percent_rank ASC NULLS LAST, id) AS next_max_voltage_so_far,
        ROUND(AVG(kwh_decile) OVER (PARTITION BY YEAR(read_at) ORDER BY kwh_seq DESC, id ROWS BETWEEN 1 PRECEDING AND 1 PRECEDING), 2) AS kwh_decile_avg_1_prior,
        COUNT(*) OVER w_read_at_to_date AS rows_in_read_at_to_date,
        SIZE(COLLECT_SET(site_zone) OVER w_read_at_to_date) AS distinct_site_zone_in_read_at_to_date,
        ROUND((UNIX_TIMESTAMP(last_known_read_at) - UNIX_TIMESTAMP(LAG(last_known_read_at) OVER (ORDER BY max_voltage_so_far, id))) / 60, 1) AS minutes_since_prev_last_known_read_at,
        NTILE(3) OVER w_all_by_max_voltage_so_far AS max_voltage_so_far_tercile
    FROM leaders
    WINDOW w_read_at_to_date AS (PARTITION BY TO_DATE(read_at)), w_all_by_max_voltage_so_far AS (ORDER BY max_voltage_so_far, id)
)
-- Final projection
SELECT
    id,
    read_at,
    quality,
    quality_slug,
    quality_site_zone_sum_kwh,
    read_at_cume_dist,
    second_voltage,
    read_at_percent_rank,
    running_count,
    kwh_decile,
    kwh_rank,
    kwh_seq,
    last_known_read_at,
    next_max_voltage_so_far,
    kwh_decile_avg_1_prior,
    rows_in_read_at_to_date,
    minutes_since_prev_last_known_read_at,
    max_voltage_so_far_tercile,
    CONCAT(quality_slug, '#', CAST(kwh_rank AS STRING)) AS quality_slug_ref,
    LOWER(TRIM(site_zone)) AS site_zone_clean
FROM ranked
