WITH cleaned AS (
    -- Scope the meter_readings rows
    SELECT
        id,
        site_id,
        read_at,
        kwh,
        voltage,
        quality,
        CASE WHEN quality IN ('OK', 'EST') THEN 'group_a' WHEN quality = 'BAD' THEN 'group_b' ELSE 'other' END AS quality_group,
        kwh + voltage AS kwh_plus_voltage,
        DATE_TRUNC('QUARTER', read_at) AS read_at_quarter
    FROM meter_readings
    WHERE quality = 'OK'
),
windowed AS (
    -- Running and moving aggregates
    SELECT
        id,
        site_id,
        read_at,
        kwh,
        voltage,
        quality,
        quality_group,
        kwh_plus_voltage,
        ROUND(AVG(kwh) OVER (ORDER BY read_at, id ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING), 2) AS kwh_avg_3_prior,
        RANK() OVER (ORDER BY read_at_quarter, id) AS read_at_quarter_rank,
        SUM(kwh_plus_voltage) OVER w_read_at_year AS read_at_year_sum_kwh_plus_voltage,
        MAX(voltage) OVER (PARTITION BY YEAR(read_at)) AS read_at_year_max_voltage,
        NTILE(5) OVER w_all_by_read_at AS read_at_quintile,
        ROUND((UNIX_TIMESTAMP(read_at) - UNIX_TIMESTAMP(LAG(read_at) OVER w_all_by_read_at)) / 60, 1) AS minutes_since_prev_read_at
    FROM cleaned
    WINDOW w_read_at_year AS (PARTITION BY YEAR(read_at)), w_all_by_read_at AS (ORDER BY read_at, id)
),
windowed_2 AS (
    -- Running and moving aggregates
    SELECT
        id,
        site_id,
        read_at,
        kwh,
        voltage,
        quality,
        kwh_avg_3_prior,
        read_at_year_max_voltage,
        minutes_since_prev_read_at,
        ROUND(AVG(minutes_since_prev_read_at) OVER (ORDER BY voltage NULLS LAST, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW), 2) AS minutes_since_prev_read_at_avg_7,
        RANK() OVER (ORDER BY voltage NULLS LAST) AS voltage_rank,
        ROUND(read_at_quintile - AVG(read_at_quintile) OVER (PARTITION BY quality, quality_group), 2) AS read_at_quintile_vs_quality_quality_group_avg,
        MAX(voltage) OVER (ORDER BY voltage NULLS LAST, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS max_voltage_so_far
    FROM windowed
)
SELECT
    id,
    site_id,
    read_at,
    kwh,
    voltage,
    quality,
    kwh_avg_3_prior,
    voltage_rank,
    read_at_quintile_vs_quality_quality_group_avg,
    max_voltage_so_far,
    DATE_TRUNC('WEEK', read_at) AS read_at_week
FROM windowed_2
WHERE read_at_year_max_voltage > 994.0
ORDER BY max_voltage_so_far DESC NULLS FIRST, id
LIMIT 10
