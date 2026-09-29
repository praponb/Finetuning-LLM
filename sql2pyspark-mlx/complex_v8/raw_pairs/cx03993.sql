WITH cleaned_readings AS (
    -- Scope the meter_readings rows
    SELECT
        id,
        site_id,
        read_at,
        kwh,
        voltage,
        DATE_FORMAT(read_at, 'yyyy-MM') AS read_at_label,
        ROUND(kwh / NULLIF(voltage, 0), 3) AS kwh_per_voltage,
        SUBSTRING(TRIM(quality), 1, 2) AS quality_prefix
    FROM meter_readings
    WHERE quality <> 'OK'
      AND TO_DATE(read_at) < DATE '2025-03-08'
),
ranked AS (
    -- Running and moving aggregates
    SELECT
        id,
        site_id,
        read_at,
        kwh,
        voltage,
        DENSE_RANK() OVER (ORDER BY read_at DESC) AS read_at_dense_rank,
        voltage - LAG(voltage) OVER w_all_by_read_at AS voltage_change,
        SUM(kwh) OVER (PARTITION BY site_id) AS site_id_sum_kwh,
        MIN(kwh) OVER (PARTITION BY site_id) AS site_id_min_kwh,
        DENSE_RANK() OVER w_all_by_read_at AS read_at_dense_rank_2,
        ROUND(CUME_DIST() OVER (ORDER BY read_at DESC, id), 3) AS read_at_cume_dist
    FROM cleaned_readings
    WINDOW w_all_by_read_at AS (ORDER BY read_at DESC, id)
),
labeled AS (
    -- Label and score rows
    SELECT
        id,
        read_at,
        kwh,
        voltage,
        voltage_change,
        site_id_min_kwh,
        voltage + kwh AS voltage_plus_kwh
    FROM ranked
    WHERE site_id_min_kwh BETWEEN 360.0 AND 1020.0
),
annotated AS (
    -- Label and score rows
    SELECT
        *,
        DATE_TRUNC('DAY', read_at) AS read_at_day,
        FLOOR(voltage_plus_kwh / 50) * 50 AS voltage_plus_kwh_bucket,
        DATEDIFF(DATE '2025-06-30', read_at) AS days_since_read_at
    FROM labeled
),
with_trends AS (
    -- Rank and compare rows within groups
    SELECT
        id,
        kwh,
        voltage,
        voltage_change,
        voltage_plus_kwh,
        read_at_day,
        voltage_plus_kwh_bucket,
        days_since_read_at,
        MAX(voltage_change) OVER w_read_at_year AS read_at_year_max_voltage_change,
        FIRST_VALUE(site_id_min_kwh) OVER (PARTITION BY YEAR(read_at) ORDER BY voltage_change, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_site_id_min_kwh,
        ROUND(CUME_DIST() OVER (PARTITION BY YEAR(read_at) ORDER BY voltage_change), 3) AS voltage_change_cume_dist,
        ROUND(AVG(site_id_min_kwh) OVER (PARTITION BY YEAR(read_at) ORDER BY voltage_change, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW), 2) AS site_id_min_kwh_avg_7
    FROM annotated
    WINDOW w_read_at_year AS (PARTITION BY YEAR(read_at))
),
sequenced AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        ROUND(AVG(days_since_read_at) OVER (ORDER BY voltage DESC NULLS FIRST, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW), 2) AS days_since_read_at_avg_3,
        LEAD(days_since_read_at) OVER w_all_by_voltage AS next_days_since_read_at,
        MAX(site_id_min_kwh_avg_7) OVER (ORDER BY voltage DESC NULLS FIRST, id ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING) AS site_id_min_kwh_avg_7_max_6_prior,
        ROUND(CUME_DIST() OVER w_all_by_voltage, 3) AS voltage_cume_dist,
        ROUND(PERCENT_RANK() OVER (ORDER BY voltage_plus_kwh ASC, id), 3) AS voltage_plus_kwh_percent_rank
    FROM with_trends
    WINDOW w_all_by_voltage AS (ORDER BY voltage DESC NULLS FIRST, id)
),
windowed AS (
    -- Previous / next values per key
    SELECT
        *,
        NTILE(10) OVER (PARTITION BY MONTH(read_at_day) ORDER BY voltage_change ASC, id) AS voltage_change_decile,
        ROUND(CUME_DIST() OVER (PARTITION BY MONTH(read_at_day) ORDER BY voltage_change ASC, id), 3) AS voltage_change_cume_dist_2
    FROM sequenced
)
-- Final projection
SELECT
    id,
    voltage_change,
    voltage_plus_kwh,
    read_at_day,
    voltage_plus_kwh_bucket,
    read_at_year_max_voltage_change,
    first_site_id_min_kwh,
    voltage_change_cume_dist,
    site_id_min_kwh_avg_7,
    next_days_since_read_at,
    site_id_min_kwh_avg_7_max_6_prior,
    voltage_cume_dist,
    QUARTER(read_at_day) AS read_at_day_quarter
FROM windowed
WHERE voltage_change_cume_dist > 0.3
ORDER BY read_at_day DESC, voltage_change DESC
