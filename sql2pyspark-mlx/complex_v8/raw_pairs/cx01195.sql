WITH recent_readings AS (
    -- Filter meter_readings
    SELECT
        id,
        site_id,
        read_at,
        kwh,
        voltage,
        quality
    FROM meter_readings
    WHERE quality = 'OK'
      AND voltage < 240.1
),
with_windows AS (
    -- Running and moving aggregates
    SELECT
        *,
        ROUND((UNIX_TIMESTAMP(read_at) - UNIX_TIMESTAMP(LAG(read_at) OVER (PARTITION BY YEAR(read_at) ORDER BY voltage, id))) / 60, 1) AS minutes_since_prev_read_at,
        SIZE(COLLECT_SET(quality) OVER (PARTITION BY site_id)) AS distinct_quality_in_site_id,
        MAX(kwh) OVER (PARTITION BY quality, TO_DATE(read_at) ORDER BY read_at DESC NULLS FIRST, id ROWS BETWEEN 1 PRECEDING AND 1 PRECEDING) AS kwh_max_1_prior,
        LAST_VALUE(voltage) IGNORE NULLS OVER (PARTITION BY site_id, quality ORDER BY voltage ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_voltage,
        LEAD(kwh) OVER (ORDER BY read_at, id) AS next_kwh,
        LAST_VALUE(read_at) IGNORE NULLS OVER (PARTITION BY site_id, quality ORDER BY voltage ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_read_at,
        SUM(kwh) OVER (PARTITION BY site_id, quality ORDER BY voltage ASC, id ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) AS kwh_sum_2,
        SIZE(COLLECT_SET(site_id) OVER (PARTITION BY quality, DATE_TRUNC('MONTH', read_at))) AS distinct_site_id_in_quality_read_at_month,
        ROUND(AVG(voltage) OVER (ORDER BY read_at, id ROWS BETWEEN 1 PRECEDING AND 1 PRECEDING), 2) AS voltage_avg_1_prior,
        MAX(kwh) OVER w_site_id_quality AS site_id_quality_max_kwh,
        NTILE(3) OVER (PARTITION BY quality, site_id ORDER BY read_at DESC, id) AS read_at_tercile,
        NTILE(3) OVER (PARTITION BY quality, TO_DATE(read_at) ORDER BY read_at DESC NULLS FIRST, id) AS read_at_tercile_2,
        ROUND(kwh / NULLIF(SUM(kwh) OVER (PARTITION BY site_id), 0), 4) AS kwh_share_of_site_id,
        RANK() OVER (PARTITION BY site_id, quality ORDER BY voltage ASC) AS voltage_rank
    FROM recent_readings
    WINDOW w_site_id_quality AS (PARTITION BY site_id, quality)
),
with_windows_2 AS (
    -- Rank and compare rows within groups
    SELECT
        id,
        site_id,
        read_at,
        kwh_max_1_prior,
        last_known_voltage,
        next_kwh,
        last_known_read_at,
        distinct_site_id_in_quality_read_at_month,
        voltage_avg_1_prior,
        read_at_tercile_2,
        kwh_share_of_site_id,
        voltage_rank,
        SUM(read_at_tercile) OVER (ORDER BY last_known_read_at, id ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING) AS read_at_tercile_sum_3_prior,
        ROUND(read_at_tercile_2 - AVG(read_at_tercile_2) OVER w_site_id_read_at_tercile, 2) AS read_at_tercile_2_vs_site_id_read_at_tercile_avg,
        ROUND(PERCENT_RANK() OVER (PARTITION BY YEAR(last_known_read_at), read_at_tercile_2 ORDER BY distinct_quality_in_site_id ASC, id), 3) AS distinct_quality_in_site_id_percent_rank,
        SIZE(COLLECT_SET(site_id) OVER (PARTITION BY distinct_quality_in_site_id)) AS distinct_site_id_in_distinct_quality_in_site_id,
        ROUND(AVG(voltage_rank) OVER w_distinct_quality_in_site_id, 2) AS distinct_quality_in_site_id_avg_voltage_rank
    FROM with_windows
    WINDOW w_site_id_read_at_tercile AS (PARTITION BY site_id, read_at_tercile), w_distinct_quality_in_site_id AS (PARTITION BY distinct_quality_in_site_id)
)
SELECT
    id,
    read_at,
    kwh_max_1_prior,
    next_kwh,
    distinct_site_id_in_quality_read_at_month,
    voltage_avg_1_prior,
    read_at_tercile_2,
    read_at_tercile_sum_3_prior,
    distinct_quality_in_site_id_percent_rank,
    distinct_site_id_in_distinct_quality_in_site_id,
    distinct_quality_in_site_id_avg_voltage_rank
FROM with_windows_2
ORDER BY distinct_quality_in_site_id_percent_rank DESC, read_at_tercile_2
