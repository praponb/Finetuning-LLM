WITH scoped_readings AS (
    -- Filter meter_readings
    SELECT
        id,
        site_id,
        read_at,
        kwh,
        voltage,
        quality,
        LOWER(REGEXP_REPLACE(TRIM(quality), ' ', '_')) AS quality_slug,
        INITCAP(TRIM(quality)) AS quality_clean
    FROM meter_readings
    WHERE site_id IS NOT NULL
      AND voltage >= 220.2
),
ranked AS (
    -- Running and moving aggregates
    SELECT
        id,
        read_at,
        kwh,
        voltage,
        quality_slug,
        quality_clean,
        LAST_VALUE(voltage) IGNORE NULLS OVER (PARTITION BY site_id ORDER BY voltage, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_voltage,
        FIRST_VALUE(voltage) IGNORE NULLS OVER (PARTITION BY site_id, quality ORDER BY kwh DESC NULLS FIRST, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_voltage,
        DENSE_RANK() OVER w_site_id_quality_by_kwh AS kwh_dense_rank,
        NTILE(3) OVER (PARTITION BY site_id, MONTH(read_at) ORDER BY read_at, id) AS read_at_tercile,
        LAST_VALUE(kwh) IGNORE NULLS OVER (PARTITION BY site_id, quality ORDER BY kwh DESC NULLS FIRST, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_kwh,
        MAX(voltage) OVER (PARTITION BY site_id ORDER BY voltage, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS max_voltage_so_far,
        ROUND(PERCENT_RANK() OVER (PARTITION BY site_id, MONTH(read_at) ORDER BY read_at), 3) AS read_at_percent_rank,
        RANK() OVER (PARTITION BY quality ORDER BY read_at NULLS LAST, id) AS read_at_rank,
        ROUND(kwh - AVG(kwh) OVER w_site_id, 2) AS kwh_vs_site_id_avg,
        SUM(kwh) OVER (PARTITION BY site_id) AS site_id_sum_kwh,
        LAG(voltage) OVER (ORDER BY read_at ASC, id) AS prev_voltage,
        ROUND(PERCENT_RANK() OVER w_all_by_read_at, 3) AS read_at_percent_rank_2
    FROM scoped_readings
    WINDOW w_site_id_quality_by_kwh AS (PARTITION BY site_id, quality ORDER BY kwh DESC NULLS FIRST, id), w_site_id AS (PARTITION BY site_id), w_all_by_read_at AS (ORDER BY read_at ASC, id)
),
top_per_group AS (
    -- Top 5 per group
    SELECT *
    FROM ranked
    WHERE kwh_dense_rank <= 5
      AND read_at_percent_rank < 0.9
)
-- Final projection
SELECT
    id,
    kwh,
    voltage,
    quality_slug,
    quality_clean,
    first_known_voltage,
    kwh_dense_rank,
    read_at_tercile,
    last_known_kwh,
    max_voltage_so_far,
    prev_voltage,
    read_at_percent_rank_2,
    TO_DATE(read_at) AS read_at_date
FROM top_per_group
