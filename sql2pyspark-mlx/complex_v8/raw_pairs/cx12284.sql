WITH base AS (
    -- Filter meter_readings
    SELECT
        id,
        site_id,
        read_at,
        kwh,
        voltage,
        quality,
        COALESCE(quality, 'UNKNOWN') AS quality_or_unknown
    FROM meter_readings
    WHERE voltage >= 218.4
      AND site_id <= 7
),
with_trends AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        NTILE(4) OVER w_site_id_by_read_at AS read_at_quartile,
        NTILE(3) OVER w_all_by_kwh AS kwh_tercile,
        FIRST_VALUE(kwh) IGNORE NULLS OVER (ORDER BY kwh DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_kwh,
        SUM(voltage) OVER (ORDER BY read_at DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_voltage,
        LEAD(read_at, 3) OVER (ORDER BY read_at DESC, id) AS next3_read_at,
        ROW_NUMBER() OVER (ORDER BY kwh DESC, id) AS seq_no,
        ROUND((UNIX_TIMESTAMP(read_at) - UNIX_TIMESTAMP(LAG(read_at) OVER w_all_by_kwh)) / 60, 1) AS minutes_since_prev_read_at,
        LAG(read_at) OVER w_all_by_kwh AS prev_read_at,
        FIRST_VALUE(voltage) IGNORE NULLS OVER (ORDER BY kwh DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_voltage,
        ROUND(kwh / NULLIF(SUM(kwh) OVER w_site_id, 0), 4) AS kwh_share_of_site_id,
        FIRST_VALUE(read_at) OVER (ORDER BY read_at DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_read_at,
        MAX(voltage) OVER (PARTITION BY site_id) AS site_id_max_voltage
    FROM base
    WINDOW w_site_id_by_read_at AS (PARTITION BY site_id ORDER BY read_at NULLS LAST, id), w_all_by_kwh AS (ORDER BY kwh DESC, id), w_site_id AS (PARTITION BY site_id)
),
metrics AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        SIZE(COLLECT_SET(quality) OVER (PARTITION BY quality_or_unknown)) AS distinct_quality_in_quality_or_unknown,
        kwh_share_of_site_id - LAG(kwh_share_of_site_id) OVER (PARTITION BY quality_or_unknown ORDER BY minutes_since_prev_read_at DESC, id) AS kwh_share_of_site_id_change,
        ROUND(first_known_kwh / NULLIF(SUM(first_known_kwh) OVER (PARTITION BY YEAR(next3_read_at), site_id), 0), 4) AS first_known_kwh_share_of_next3_read_at_year_site,
        ROW_NUMBER() OVER (PARTITION BY kwh_tercile ORDER BY read_at ASC, id) AS read_at_seq
    FROM with_trends
)
SELECT
    id,
    kwh,
    voltage,
    quality_or_unknown,
    kwh_tercile,
    first_known_kwh,
    seq_no,
    minutes_since_prev_read_at,
    prev_read_at,
    first_known_voltage,
    first_read_at,
    site_id_max_voltage,
    distinct_quality_in_quality_or_unknown,
    kwh_share_of_site_id_change,
    read_at_seq,
    ROUND(first_known_kwh * 1.5, 2) AS first_known_kwh_adj
FROM metrics
WHERE first_known_kwh_share_of_next3_read_at_year_site BETWEEN 0.1 AND 0.9
ORDER BY voltage
